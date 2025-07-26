import os
import duckdb

def main(db_path, excel_path):
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 9: DB file not found at '{db_path}'.")
        return
    con = duckdb.connect(db_path)
    try:
        con.execute("ALTER TABLE final_data ADD COLUMN IF NOT EXISTS COURT_NAME VARCHAR;")
        con.execute("""
            UPDATE final_data SET COURT_NAME = CASE
                WHEN UPPER(FILENAME) LIKE '%2ADJ%' THEN '2ADJ' WHEN UPPER(FILENAME) LIKE '%3SD%' THEN '3SD'
                WHEN UPPER(FILENAME) LIKE '%4SD%' THEN '4SD' WHEN UPPER(FILENAME) LIKE '%2SD%' THEN '2SD'
                WHEN UPPER(FILENAME) LIKE '%1SD%' THEN '1SD' WHEN UPPER(FILENAME) LIKE '%CHIEF%' THEN 'CHIEF'
                WHEN UPPER(FILENAME) LIKE '%MSD%' THEN 'MSD' WHEN UPPER(FILENAME) LIKE '%PDJ%' THEN 'PDJ'
                WHEN UPPER(FILENAME) LIKE '%ADJ%' THEN 'ADJ' WHEN UPPER(FILENAME) LIKE '%1JD%' THEN '1JD'
                WHEN UPPER(FILENAME) LIKE '%2JD%' THEN '2JD' WHEN UPPER(FILENAME) LIKE '%3JD%' THEN '3JD'
                WHEN UPPER(FILENAME) LIKE '%4JD%' THEN '4JD' WHEN UPPER(FILENAME) LIKE '%5JD%' THEN '5JD'
                WHEN UPPER(FILENAME) LIKE '%SPECIAL%' THEN 'SPECIAL' WHEN UPPER(FILENAME) LIKE '%SESSIONS%' THEN 'SESSIONS'
                WHEN UPPER(FILENAME) LIKE '%RAN%' THEN 'RAN' WHEN UPPER(FILENAME) LIKE '%KUT%' THEN 'KUT'
                WHEN UPPER(FILENAME) LIKE '%PBR%' THEN 'PBR' ELSE 'UNKNOWN'
            END;
        """)
        df = con.execute("SELECT * FROM final_data").fetchdf()
        df.to_excel(excel_path, index=False)
        print("✅ STEP 9: COURT_NAME column created successfully.")
    finally:
        con.close()