import os
import duckdb

def main(db_path, excel_path):
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 8: DB file not found at '{db_path}'.")
        return
    con = duckdb.connect(db_path)
    try:
        # ESTA
        con.execute("ALTER TABLE final_data ADD COLUMN IF NOT EXISTS ESTA VARCHAR;")
        con.execute("""
            UPDATE final_data SET ESTA = CASE 
                WHEN UPPER(FILENAME) LIKE '%RAN%' THEN 'RAN'
                WHEN UPPER(FILENAME) LIKE '%KUT%' THEN 'KUT'
                WHEN UPPER(FILENAME) LIKE '%PBR%' THEN 'PBR'
                ELSE 'PBR'
            END;
        """)
        # UID
        con.execute("ALTER TABLE final_data ADD COLUMN IF NOT EXISTS UID VARCHAR;")
        con.execute("""
            UPDATE final_data SET UID = CASE
                WHEN ESTA IS NOT NULL AND TRIM(ESTA) != '' AND 
                     CASE_NO IS NOT NULL AND TRIM(CASE_NO) != ''
                THEN TRIM(ESTA) || '/' || TRIM(CASE_NO)
                ELSE NULL
            END;
        """)
        df = con.execute("SELECT * FROM final_data").fetchdf()
        df.to_excel(excel_path, index=False)
        print("✅ STEP 8: ESTA and UID columns created successfully.")
    finally:
        con.close()