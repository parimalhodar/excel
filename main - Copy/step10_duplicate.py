# step10_duplicate.py

import os
import duckdb
import pandas as pd

def main(db_path, excel_path):
    """
    Removes duplicate records from the database based on the 'UID' column.
    It keeps the first occurrence of each UID.

    Args:
        db_path (str): The full path to the DuckDB database file.
        excel_path (str): The full path to the Excel file to overwrite.
    """
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 10: DB file not found at '{db_path}'.")
        return

    con = None
    try:
        con = duckdb.connect(db_path)
        
        # --- THIS IS THE FIX ---
        # Safely get the initial count
        result = con.execute("SELECT COUNT(*) FROM final_data").fetchone()
        initial_count = result[0] if result else 0

        if initial_count == 0:
            print("   - No data to process for duplicates. Skipping.")
            return
            
        df = con.execute("SELECT * FROM final_data").fetchdf()
        
        # Check if UID column exists before trying to drop duplicates
        if 'UID' not in df.columns:
            print("   - ⚠️ Warning in Step 10: 'UID' column not found. Cannot remove duplicates.")
            df.to_excel(excel_path, index=False)
            return

        # Drop duplicates based on the 'UID' column, keeping the first one found
        df.drop_duplicates(subset=['UID'], keep='first', inplace=True)
        
        # Overwrite the table in the database with the de-duplicated data
        con.execute("DROP TABLE IF EXISTS final_data")
        con.execute("CREATE TABLE final_data AS SELECT * FROM df")
        
        # --- THIS IS THE FIX ---
        # Safely get the final count
        result = con.execute("SELECT COUNT(*) FROM final_data").fetchone()
        final_count = result[0] if result else 0

        # Save the final, de-duplicated data to the Excel file
        df.to_excel(excel_path, index=False)
        
        print(f"✅ STEP 10: Duplicates removed. Records changed from {initial_count} to {final_count}.")

    except Exception as e:
        print(f"❌ An error occurred during database operations in Step 10: {e}")
    finally:
        if con:
            con.close()