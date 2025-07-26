# step07_side.py

import os
import duckdb

def process_data(db_path):
    """
    Adds a 'SIDE' column (CIVIL/CRIMINAL) to the database based on a
    mapping of values in the 'CAT1' column. This step only modifies the
    database file.

    Args:
        db_path (str): The full path to the DuckDB database file to modify.
    
    Returns:
        bool: True if successful, False otherwise.
    """
    # 1. Validate that the database file from the previous step exists
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 7: Database file not found at '{db_path}'. Cannot proceed.")
        return False

    con = None  # Initialize connection to None
    try:
        # 2. Connect to the existing database
        con = duckdb.connect(db_path)

        # 3. Check if the required source column 'CAT1' exists
        columns = con.execute("PRAGMA table_info('final_data');").fetchdf()
        if 'cat1' not in columns['name'].str.lower().tolist():
            print("   - ⚠️ Warning in Step 7: 'CAT1' column not found. Cannot determine case SIDE.")
            return True # Return True to allow pipeline to continue

        # 4. Add the 'SIDE' column if it doesn't already exist
        if 'side' not in columns['name'].str.lower().tolist():
            con.execute("ALTER TABLE final_data ADD COLUMN SIDE VARCHAR;")
            print("   - 'SIDE' column created.")
        else:
            print("   - 'SIDE' column already exists. Values will be updated.")

        # 5. Define the mapping from CAT1 to SIDE using a SQL CASE statement
        mapping_sql = """
        CASE
            WHEN "CAT1" = 'MACP' THEN 'CIVIL'
            WHEN "CAT1" = 'ATRO' THEN 'CRIMINAL'
            WHEN "CAT1" = 'TADA' THEN 'CRIMINAL'
            WHEN "CAT1" = 'CMA DC' THEN 'CIVIL'
            WHEN "CAT1" = 'SC' THEN 'CRIMINAL'
            WHEN "CAT1" = 'CR A' THEN 'CRIMINAL'
            WHEN "CAT1" = 'RCA' THEN 'CIVIL'
            WHEN "CAT1" = 'CR RA' THEN 'CRIMINAL'
            WHEN "CAT1" = 'MACEX' THEN 'CIVIL'
            WHEN "CAT1" = 'ACB' THEN 'CRIMINAL'
            WHEN "CAT1" = 'MACMA' THEN 'CIVIL'
            WHEN "CAT1" = 'COMM EX' THEN 'CIVIL'
            WHEN "CAT1" = 'PCSO' THEN 'CRIMINAL'
            WHEN "CAT1" = 'GLGP' THEN 'CRIMINAL'
            WHEN "CAT1" = 'GLGPCIVIL' THEN 'CIVIL'
            WHEN "CAT1" = 'SMRY' THEN 'CIVIL'
            WHEN "CAT1" = 'EXE S' THEN 'CIVIL'
            WHEN "CAT1" = 'ELEC' THEN 'CRIMINAL'
            WHEN "CAT1" = 'MCA' THEN 'CIVIL'
            WHEN "CAT1" = 'CRMA S' THEN 'CRIMINAL'
            WHEN "CAT1" = 'NDPS' THEN 'CRIMINAL'
            WHEN "CAT1" = 'REWA' THEN 'CIVIL'
            WHEN "CAT1" = 'GPID CC' THEN 'CRIMINAL'
            WHEN "CAT1" = 'CR EN' THEN 'CRIMINAL'
            WHEN "CAT1" = 'TMSUIT' THEN 'CIVIL'
            WHEN "CAT1" = 'CC' THEN 'CRIMINAL'
            WHEN "CAT1" = 'SPCS' THEN 'CIVIL'
            WHEN "CAT1" = 'CMA SC' THEN 'CIVIL'
            WHEN "CAT1" = 'RCS' THEN 'CIVIL'
            WHEN "CAT1" = 'CRMA J' THEN 'CRIMINAL'
            WHEN "CAT1" = 'CC JUVE' THEN 'CRIMINAL'
            WHEN "CAT1" = 'EXE LAR' THEN 'CIVIL'
            WHEN "CAT1" = 'EXE R' THEN 'CIVIL'
            WHEN "CAT1" = 'SMST R' THEN 'CIVIL'
            WHEN "CAT1" = 'SMST S' THEN 'CIVIL'
            WHEN "CAT1" = 'LAR' THEN 'CIVIL'
            WHEN "CAT1" = 'COMM CS' THEN 'CIVIL'
            WHEN "CAT1" = 'HMP' THEN 'CIVIL'
            ELSE 'UNKNOWN'
        END
        """

        # 6. Execute the update query
        con.execute(f"""
            UPDATE final_data
            SET "SIDE" = {mapping_sql}
        """)

        print("✅ STEP 7: SIDE column classified successfully.")
        return True

    except Exception as e:
        print(f"❌ An error occurred during database operations in Step 7: {e}")
        return False
    finally:
        # 7. Ensure the database connection is always closed
        if con:
            con.close()