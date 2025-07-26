# app.py

import os
import shutil
import uuid
import traceback
import io
import pandas as pd
from flask import (
    Flask, request, render_template, send_file,
    flash, redirect, url_for, g
)
import run_all

# --- Flask App Initialization ---

# Create the Flask application instance.
# By setting template_folder='.', we tell Flask to look for HTML files
# in the same directory as this script, instead of a 'templates' subfolder.
app = Flask(__name__, template_folder='.')

# A secret key is needed for showing flashed messages to the user.
app.config['SECRET_KEY'] = 'a-super-secret-key-that-is-hard-to-guess'

# Define folders for temporary file storage.
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# --- Automatic Cleanup Function ---

@app.teardown_request
def teardown_request(exception=None):
    """
    This function runs automatically after each request is finished.
    It cleans up the temporary folders created for that request.
    """
    # 'g' is a special Flask object that is unique for each request.
    # We use it to store the paths of the folders we need to clean up.
    upload_path = g.pop('upload_path', None)
    output_path = g.pop('output_path', None)

    if upload_path and os.path.exists(upload_path):
        print(f"Cleaning up upload folder: {upload_path}")
        shutil.rmtree(upload_path)
    
    if output_path and os.path.exists(output_path):
        print(f"Cleaning up output folder: {output_path}")
        shutil.rmtree(output_path)


# --- Main Application Route (Upload Page) ---

@app.route('/', methods=['GET', 'POST'])
def index():
    # This block handles the file upload when the user clicks the "Process" button.
    if request.method == 'POST':
        # Basic validation to ensure files were actually submitted.
        if 'files[]' not in request.files or not request.files.getlist('files[]')[0].filename:
            flash('No files selected. Please select one or more Excel files.')
            return redirect(request.url)

        files = request.files.getlist('files[]')

        # Create unique temporary directories for this specific request.
        session_id = str(uuid.uuid4())
        session_upload_path = os.path.join(UPLOAD_FOLDER, session_id)
        session_output_path = os.path.join(OUTPUT_FOLDER, session_id)
        os.makedirs(session_upload_path, exist_ok=True)
        os.makedirs(session_output_path, exist_ok=True)

        # Store these paths in the 'g' object so the teardown function can find them later.
        g.upload_path = session_upload_path
        g.output_path = session_output_path

        try:
            # Save the user's uploaded files to our temporary upload folder.
            for file in files:
                if file and file.filename and file.filename.endswith('.xlsx'):
                    file.save(os.path.join(session_upload_path, file.filename))
            
            # Call the main processing pipeline, which will do all the work
            # and return the final data as a pandas DataFrame.
            print(f"Starting pipeline for session {session_id}...")
            final_df = run_all.main(
                source_directory=session_upload_path,
                output_directory=session_output_path
            )

            # Create an in-memory binary buffer to hold the Excel file data.
            output_buffer = io.BytesIO()

            # Save the final DataFrame to the in-memory buffer as an Excel file.
            # This avoids writing the final file to disk, preventing any file lock errors.
            final_df.to_excel(output_buffer, index=False, engine='openpyxl')

            # Rewind the buffer's "cursor" to the beginning so Flask can read it.
            output_buffer.seek(0)

            # Send the in-memory file directly to the user's browser for download.
            print("Pipeline finished. Sending file from memory.")
            return send_file(
                output_buffer,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='processed_data.xlsx'
            )

        except Exception as e:
            # If any error occurs during the process, show it to the user.
            traceback.print_exc()
            flash(f'An error occurred during processing: {e}')
            return redirect(request.url)

    # This handles the initial page load (a GET request).
    # It just shows the upload form.
    return render_template('index.html')


# --- This block runs the Flask web server ---
if __name__ == '__main__':
    # debug=True provides helpful error pages and auto-reloads the server when you save changes.
    app.run(debug=True)
