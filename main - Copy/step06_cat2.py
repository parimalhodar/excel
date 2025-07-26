import os
import duckdb

def main(db_path, excel_path):
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 6: DB file not found at '{db_path}'.")
        return
    con = duckdb.connect(db_path)
    try:
        # CAT1
        con.execute("ALTER TABLE final_data ADD COLUMN IF NOT EXISTS CAT1 VARCHAR;")
        con.execute("""
            UPDATE final_data SET CAT1 = LEFT(CASE_NO, INSTR(CASE_NO, '/') - 1)
            WHERE CASE_NO IS NOT NULL AND INSTR(CASE_NO, '/') > 0;
        """)
        # CAT2
        con.execute('ALTER TABLE final_data ADD COLUMN IF NOT EXISTS "CAT2" VARCHAR;')
        con.execute("""
            UPDATE final_data SET "CAT2" = TRIM(CONCAT_WS('/', NULLIF(TRIM("CAT1"), ''),
                                  NULLIF(TRIM("NATURE"), ''), NULLIF(TRIM("IPC SPECIAL"), '')));
        """)
        con.execute("""UPDATE final_data SET "CAT2" = REGEXP_REPLACE(TRIM("CAT2"), '/+', '/');""")
        con.execute("""UPDATE final_data SET "CAT2" = TRIM(BOTH '/' FROM "CAT2");""")
        
        df = con.execute("SELECT * FROM final_data").fetchdf()
        df.to_excel(excel_path, index=False)
        print("✅ STEP 6: CAT1 and CAT2 columns created successfully.")
    finally:
        con.close()