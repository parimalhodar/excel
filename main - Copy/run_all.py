# run_all.py

import os
import duckdb
import pandas as pd
import step01_merge, step02_status, step03_date, step04_age, step05_ipc2
import step06_cat2, step07_side, step08_esta, step09_court, step10_duplicate

def main(source_directory, output_directory):
    """
    Runs the entire pipeline and returns the final data as a pandas DataFrame.

    Args:
        source_directory (str): Folder with user's uploaded Excel files.
        output_directory (str): Temporary folder for intermediate files.

    Returns:
        pd.DataFrame: The final, processed data.
    """
    final_df = None
    db_path = os.path.join(output_directory, 'final_output.duckdb')
    # The intermediate excel_path is still useful for the step scripts
    excel_path = os.path.join(output_directory, 'final_output.xlsx')
    con = None

    try:
        # --- Run all processing steps ---
        # Each step reads and writes to the temporary DuckDB file.
        step01_merge.main(source_directory, db_path, excel_path)
        step02_status.main(db_path, excel_path)
        step03_date.main(db_path, excel_path)
        step04_age.main(db_path, excel_path)
        step05_ipc2.process_data(db_path)
        step06_cat2.main(db_path, excel_path)
        step07_side.process_data(db_path)
        step08_esta.main(db_path, excel_path)
        step09_court.main(db_path, excel_path)
        step10_duplicate.main(db_path, excel_path)
        
        print("✅ All processing steps complete.")

        # --- THIS IS THE KEY CHANGE ---
        # 1. Connect to the final database.
        con = duckdb.connect(db_path, read_only=True)
        # 2. Read the final results into a DataFrame.
        final_df = con.execute("SELECT * FROM final_data").fetchdf()
        print(f"   - Final data fetched from database ({len(final_df)} rows).")

    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED during pipeline execution: {e}")
        # Re-raise the exception to be caught by the Flask app
        raise
    finally:
        if con:
            con.close()

    # 3. Return the DataFrame to the Flask app.
    return final_df