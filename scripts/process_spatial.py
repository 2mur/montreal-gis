import os
import tempfile
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
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    engine = get_db_engine()

    s5p_config = {
        "ch4": "methane_mixing_ratio_bias_corrected", 
        "no2": "nitrogendioxide_tropospheric_column",
        "o3":  "ozone_total_vertical_column",
        "co":  "carbonmonoxide_total_column",
        "so2": "sulfurdioxide_total_vertical_column"
    }

    for pollutant, var_name in s5p_config.items():
        blobs = list(bucket.list_blobs(prefix=f"sentinel-5p/{pollutant}/"))
        if not blobs:
            continue
            
        latest_blob = max(blobs, key=lambda b: b.time_created)
        _, temp_path = tempfile.mkstemp(suffix=".nc")
        latest_blob.download_to_filename(temp_path)

        try:
            ds = xr.open_dataset(temp_path, group='PRODUCT')
            
            if var_name not in ds and pollutant == "ch4":
                var_name = "methane_mixing_ratio"

            # Explicitly select the variable and the coordinates to prevent KeyErrors
            ds_subset = ds[[var_name, 'longitude', 'latitude']].isel(time=0)
            df = ds_subset.to_dataframe().reset_index()
            
            df = df.dropna(subset=[var_name, 'longitude', 'latitude'])

            df = df[
                (df['longitude'] >= -73.97) & (df['longitude'] <= -73.47) &
                (df['latitude'] >= 45.41) & (df['latitude'] <= 45.71)
            ]

            if df.empty:
                print(f"No Sentinel-5P {pollutant.upper()} data found over Montreal.")
                continue

            # Some products name the time column differently after reset_index
            time_col = 'time' if 'time' in df.columns else 'time_utc' if 'time_utc' in df.columns else None
            df['timestamp'] = pd.to_datetime(df[time_col]) if time_col else pd.Timestamp.now(tz='UTC')
            
            df = df.rename(columns={var_name: 'measurement_value'})
            df['parameter'] = pollutant

            # Create geometries in standard degrees (WGS 84)
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
            
            # Project to Montreal metric (UTM Zone 18N) for accurate buffering
            gdf = gdf.to_crs(epsg=32618)
            
            # Buffer the ACTIVE geometry directly (replaces points with 2500m square polygons)
            gdf['geometry'] = gdf.geometry.buffer(2500, cap_style=3)
            
            # Project the buffered polygons back to EPSG:4326
            gdf = gdf.to_crs(epsg=4326)
            
            # Rename the active geometry column to 'geom' to match the PostGIS table
            gdf = gdf.rename_geometry('geom')
            
            gdf = gdf[['timestamp', 'parameter', 'measurement_value', 'geom']]

            gdf.to_postgis('satellite_measurements', engine, if_exists='append', index=False)
            print(f"Successfully loaded {len(gdf)} {pollutant.upper()} satellite polygons to PostGIS.")

        except Exception as e:
            print(f"Error processing {pollutant}: {e}")
        finally:
            os.remove(temp_path)

def process_openaq():
    print("Fetching OpenAQ data from GCS...")
    
    # Changed from JSON to CSV
    csv_file_path = download_from_gcs("openaq/latest_measurements.csv", ".csv")
    
    if not csv_file_path:
        return

    try:
        # Load the flat CSV directly
        df = pd.read_csv(csv_file_path)
        
        if df.empty:
            print("No OpenAQ measurements to process.")
            return

        # Map the new flat CSV columns
        df = df.rename(columns={
            'location': 'sensor_name',
            'value': 'measurement_value'
        })
        
        df['timestamp'] = pd.to_datetime(df['utc_time'])
        df = df.dropna(subset=['lon', 'lat'])

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
        gdf = gdf.rename(columns={'geometry': 'geom'}).set_geometry('geom')
        
        gdf = gdf[['sensor_name', 'timestamp', 'parameter', 'measurement_value', 'unit', 'geom']]

        engine = get_db_engine()
        gdf.to_postgis('openaq_data', engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(gdf)} OpenAQ points to PostGIS.")

    except Exception as e:
        print(f"Error processing OpenAQ: {e}")
    finally:
        os.remove(csv_file_path)

if __name__ == "__main__":
    process_sentinel5p()
    process_openaq()