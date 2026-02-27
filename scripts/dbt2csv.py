import os
import pandas as pd
from sqlalchemy import create_engine

def export_to_csv():
    # 1. Connect to PostGIS
    db_user = os.getenv("DB_USER", "gis_user")
    db_pass = os.getenv("DB_PASS", "gis_pass")
    db_host = os.getenv("DB_HOST", "postgis") 
    db_name = os.getenv("DB_NAME", "montreal_air_quality")
    
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    engine = create_engine(db_url)
    
    # 2. Query the dbt model
    print("Extracting dbt results from PostGIS...")
    df = pd.read_sql("SELECT * FROM stg_pollutant_comparison", engine)
    
    if df.empty:
        print("Table is empty. Run dbt first.")
        return

    # 3. Clean up complex geometries for the CSV format
    # CSVs don't handle PostGIS binary geometry strings well, so we drop them 
    # to keep the CSV clean and readable.
    if 'sensor_location' in df.columns:
        df = df.drop(columns=['sensor_location', 'satellite_footprint'])
        
    # 4. Save to the local data folder
    os.makedirs("/app/data", exist_ok=True)
    file_path = "/app/data/stg_pollutant_comparison.csv"
    df.to_csv(file_path, index=False)
    
    print(f"Success! Exported {len(df)} rows to {file_path}")

if __name__ == "__main__":
    export_to_csv()