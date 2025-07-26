import os
import duckdb
import pandas as pd

def main(db_path, excel_path):
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 3: DB file not found at '{db_path}'.")
        return
    con = duckdb.connect(db_path)
    try:
        df = con.execute("SELECT * FROM final_data").fetchdf()
        def convert_to_new_date(date_series):
            return pd.to_datetime(date_series, format='%d-%m-%Y', errors='coerce')
        if 'DATE OF REG' in df.columns:
            df['NEW DATE OF REG'] = convert_to_new_date(df['DATE OF REG'])
        if 'DATE OF DIS' in df.columns:
            df['NEW DATE OF DIS'] = convert_to_new_date(df['DATE OF DIS'])
        con.execute("DROP TABLE IF EXISTS final_data")
        con.execute("CREATE TABLE final_data AS SELECT * FROM df")
        df.to_excel(excel_path, index=False)
        print("✅ STEP 3: Date columns converted successfully.")
    finally:
        con.close()