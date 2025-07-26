# app.py

import os
import shutil
import uuid
import traceback
import io  # Import the In-Memory I/O library
import pandas as pd
from flask import (
    Flask, request, render_template, send_file,
    flash, redirect, url_for, g
)
import run_all

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'a-super-secret-key-that-is-hard-to-guess'
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# --- Teardown function for cleanup ---
@app.teardown_request
def teardown_request(exception=None):
    """Cleans up the temporary folders after the request is complete."""
    upload_path = g.pop('upload_path', None)
    output_path = g.pop('output_path', None)

    if upload_path and os.path.exists(upload_path):
        shutil.rmtree(upload_path)
    
    if output_path and os.path.exists(output_path):
        shutil.rmtree(output_path)


# --- Main Application Route ---
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'files[]' not in request.files or not request.files.getlist('files[]')[0].filename:
            flash('No files selected. Please select one or more Excel files.')
            return redirect(request.url)

        files = request.files.getlist('files[]')

        session_id = str(uuid.uuid4())
        session_upload_path = os.path.join(UPLOAD_FOLDER, session_id)
        session_output_path = os.path.join(OUTPUT_FOLDER, session_id)
        os.makedirs(session_upload_path, exist_ok=True)
        os.makedirs(session_output_path, exist_ok=True)

        g.upload_path = session_upload_path
        g.output_path = session_output_path

        try:
            for file in files:
                if file and file.filename and file.filename.endswith('.xlsx'):
                    file.save(os.path.join(session_upload_path, file.filename))
            
            # --- THIS IS THE KEY CHANGE ---
            # 1. Get the final DataFrame from the pipeline.
            print(f"Starting pipeline for session {session_id}...")
            final_df = run_all.main(
                source_directory=session_upload_path,
                output_directory=session_output_path
            )

            # 2. Create an in-memory binary buffer.
            output_buffer = io.BytesIO()

            # 3. Save the DataFrame to the buffer as an Excel file.
            final_df.to_excel(output_buffer, index=False, engine='openpyxl')

            # 4. Rewind the buffer's "cursor" to the beginning.
            output_buffer.seek(0)

            # 5. Use send_file to send the buffer from memory.
            print("Pipeline finished. Sending file from memory.")
            return send_file(
                output_buffer,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='processed_data.xlsx'
            )

        except Exception as e:
            traceback.print_exc()
            flash(f'An error occurred during processing: {e}')
            return redirect(request.url)

    return render_template('index.html')


# --- This block runs the app ---
if __name__ == '__main__':
    app.run(debug=True)