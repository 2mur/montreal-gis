import os
import tempfile
import json
import xarray as xr
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from google.cloud import storage
import google.auth

_, auth_project = google.auth.default()
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", auth_project)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

DB_USER = os.getenv("DB_USER", "gis_user")
DB_PASS = os.getenv("DB_PASS", "gis_pass")
DB_HOST = os.getenv("DB_HOST", "postgis")
DB_NAME = os.getenv("DB_NAME", "montreal_methane")

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
    return create_engine(db_url)

def download_from_gcs(blob_name, suffix):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        print(f"File {blob_name} does not exist in GCS.")
        return None

    _, temp_path = tempfile.mkstemp(suffix=suffix)
    blob.download_to_filename(temp_path)
    return temp_path

def process_sentinel5p():
    print("Fetching Sentinel-5P data from GCS...")
    nc_file_path = download_from_gcs("sentinel-5p/latest.nc", ".nc")
    
    if not nc_file_path:
        return

    try:
        ds = xr.open_dataset(nc_file_path, group='PRODUCT')
        df = ds['methane_mixing_ratio'][0].to_dataframe().reset_index()
        df = df.dropna(subset=['methane_mixing_ratio']) 

        df = df[
            (df['longitude'] >= -73.97) & (df['longitude'] <= -73.47) &
            (df['latitude'] >= 45.41) & (df['latitude'] <= 45.71)
        ]

        if df.empty:
            print("No Sentinel-5P data found over Montreal for this pass.")
            return

        df['timestamp'] = pd.to_datetime(df['time'])
        df = df.rename(columns={'methane_mixing_ratio': 'ch4_column_volume'})

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
        gdf['geom'] = gdf.geometry.buffer(0.025, cap_style=3)
        gdf = gdf.set_geometry('geom')
        gdf = gdf[['timestamp', 'ch4_column_volume', 'geom']]

        engine = get_db_engine()
        gdf.to_postgis('satellite_methane', engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(gdf)} satellite polygons to PostGIS.")

    finally:
        os.remove(nc_file_path)

def process_openaq():
    print("Fetching OpenAQ data from GCS...")
    json_file_path = download_from_gcs("openaq/latest_measurements.json", ".json")
    
    if not json_file_path:
        return

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
            
        measurements = data.get("results", [])
        if not measurements:
            print("No OpenAQ measurements to process.")
            return

        df = pd.DataFrame(measurements)
        df['sensor_name'] = df['location']
        df['measurement_value'] = df['value']
        df['parameter'] = df['parameter']
        df['unit'] = df['unit']
        df['timestamp'] = pd.to_datetime(df['date'].apply(lambda x: x['utc'] if isinstance(x, dict) else x))
        
        df['lon'] = df['coordinates'].apply(lambda x: x['longitude'] if pd.notnull(x) else None)
        df['lat'] = df['coordinates'].apply(lambda x: x['latitude'] if pd.notnull(x) else None)
        df = df.dropna(subset=['lon', 'lat'])

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
        gdf = gdf.rename(columns={'geometry': 'geom'}).set_geometry('geom')
        
        gdf = gdf[['sensor_name', 'timestamp', 'parameter', 'measurement_value', 'unit', 'geom']]

        engine = get_db_engine()
        gdf.to_postgis('openaq_data', engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(gdf)} OpenAQ points to PostGIS.")

    finally:
        os.remove(json_file_path)

if __name__ == "__main__":
    process_sentinel5p()
    process_openaq()