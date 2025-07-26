# step02_status.py

import os
import duckdb
import pandas as pd

def main(db_path, excel_path):
    """
    Connects to the DuckDB database, adds and populates the STATUS column
    based on the presence of a disposal date, and saves the updated data
    back to the final Excel file.

    Args:
        db_path (str): The full path to the DuckDB database file to modify.
        excel_path (str): The full path to the Excel file to overwrite with updated data.
    """
    # 1. Validate that the database file from the previous step exists
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 2: Database file not found at '{db_path}'. Cannot proceed.")
        return

    con = None  # Initialize connection to None
    try:
        # 2. Connect to the existing database
        con = duckdb.connect(db_path)

        # 3. Check if STATUS column already exists to avoid errors on re-runs
        columns = con.execute("PRAGMA table_info('final_data');").fetchdf()
        if 'status' not in columns['name'].str.lower().tolist():
            con.execute("ALTER TABLE final_data ADD COLUMN STATUS VARCHAR;")
            print("   - 'STATUS' column created.")
        else:
            print("   - 'STATUS' column already exists. Values will be updated.")

        # 4. Update the STATUS column based on the 'DATE OF DIS' column
        # This sets STATUS to 'DISPOSE' if a disposal date exists, otherwise 'PENDING'
        con.execute("""
            UPDATE final_data 
            SET STATUS = CASE 
                WHEN "DATE OF DIS" IS NOT NULL AND TRIM("DATE OF DIS") <> '' THEN 'DISPOSE'
                ELSE 'PENDING'
            END;
        """)

        # 5. Retrieve the updated data from the table
        updated_df = con.execute("SELECT * FROM final_data").fetchdf()

        # 6. Save the updated DataFrame to the specified Excel file, overwriting it
        updated_df.to_excel(excel_path, index=False)
        
        print("✅ STEP 2: STATUS column updated successfully.")

    except Exception as e:
        print(f"❌ An error occurred during database operations in Step 2: {e}")
    finally:
        # 7. Ensure the database connection is always closed, even if errors occur
        if con:
            con.close()