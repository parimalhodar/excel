import os
import duckdb

def process_data(db_path): # Changed to accept db_path
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 5: DB file not found at '{db_path}'.")
        return False
    con = duckdb.connect(db_path)
    try:
        columns = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'final_data'").fetchdf()
        if 'ACT' not in columns['column_name'].values:
            print("   - Warning: 'ACT' column not found. Skipping IPC SPECIAL.")
            return ['IPC SPECIAL']
        if 'IPC SPECIAL' not in columns['column_name'].values:
            con.execute('ALTER TABLE final_data ADD COLUMN "IPC SPECIAL" VARCHAR')
        con.execute("""
            UPDATE final_data
            SET "IPC SPECIAL" = CASE
                WHEN "ACT" IS NULL OR TRIM("ACT") = '' THEN ''
                WHEN ("ACT" LIKE '%INDIAN PENAL CODE%467%' OR "ACT" LIKE '%INDIAN PENAL CODE%468%' OR 
                      "ACT" LIKE '%INDIAN PENAL CODE%465%' OR "ACT" LIKE '%INDIAN PENAL CODE%466%' OR 
                      "ACT" LIKE '%INDIAN PENAL CODE%471%' OR "ACT" LIKE '%THE BHARATIYA NYAYA SANHITA%333%') THEN 'IPC SPECIAL'
                ELSE ''
            END
        """)
        print("✅ STEP 5: IPC SPECIAL column processed successfully.")
        return ['IPC SPECIAL']
    finally:
        con.close()