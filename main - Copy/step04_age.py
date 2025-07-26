import os
import duckdb
import pandas as pd
from datetime import datetime

def main(db_path, excel_path):
    if not os.path.exists(db_path):
        print(f"❌ Error in Step 4: DB file not found at '{db_path}'.")
        return
    con = duckdb.connect(db_path)
    try:
        df = con.execute("SELECT * FROM final_data").fetchdf()
        df['NEW DATE OF REG'] = pd.to_datetime(df['NEW DATE OF REG'], errors='coerce')
        df['NEW DATE OF DIS'] = pd.to_datetime(df['NEW DATE OF DIS'], errors='coerce')
        today = pd.to_datetime(datetime.today().date())
        df['DATE_FOR_AGE'] = df['NEW DATE OF DIS'].fillna(today)
        df['AGE(D)'] = (df['DATE_FOR_AGE'] - df['NEW DATE OF REG']).dt.days
        df['AGE(Y)'] = (df['AGE(D)'] / 365.2425).round(2)
        df['AGE(M)'] = (df['AGE(D)'] / 30.436875).round(2)
        age_years_int = (df['AGE(D)'] // 365).astype('Int64')
        age_months_int = (df['AGE(D)'] // 30).astype('Int64')
        df['AGE CAT1'] = pd.cut(age_years_int, bins=[-1, 4, 9, 19, 29, float('inf')], labels=['0-5YR', '5-10YR', '10-20YR', '20-30YR', '30YR ABOVE'])
        def age_cat2(age):
            if pd.isna(age): return None
            if age <= 4: return '0-5 YEAR'
            elif age <= 9: return '5-10 YEAR'
            elif age <= 14: return '10-15 YEAR'
            elif age <= 19: return '15-20 YEAR'
            else: return 'MORE THAN 20 YEARS'
        df['AGE CAT2'] = age_years_int.apply(age_cat2)
        def age_cat3(age_m):
            if pd.isna(age_m): return None
            if age_m <= 3: return '0-3 MONTH'
            elif age_m <= 12: return '3-12 MONTH'
            else: return 'MORE THAN 12 MONTH'
        df['AGE CAT3'] = age_months_int.apply(age_cat3)
        df['REG MONTH'] = df['NEW DATE OF REG'].dt.strftime('%b-%Y').str.upper()
        df['DISPOSAL MONTH'] = df['NEW DATE OF DIS'].dt.strftime('%b-%Y').str.upper()
        con.execute("DROP TABLE IF EXISTS final_data")
        con.execute("CREATE TABLE final_data AS SELECT * FROM df")
        df.to_excel(excel_path, index=False)
        print("✅ STEP 4: Age columns calculated successfully.")
    finally:
        con.close()