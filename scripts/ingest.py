import os
import json
from xmlrpc import client
import zipfile
import tempfile
import shutil
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage
from google.cloud import secretmanager
import google.auth
from datetime import datetime, timezone, timedelta
from openaq import OpenAQ
import time # Add this to your imports at the top
import pandas as pd

_, auth_project = google.auth.default()
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", auth_project)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

MONTREAL_POLYGON = "POLYGON((-73.97 45.41, -73.47 45.41, -73.47 45.71, -73.97 45.71, -73.97 45.41))"

def get_http_session():
    session = requests.Session()
    retry_strategy = Retry(total=5, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

def upload_to_gcs(local_file_path, destination_blob_name):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"Successfully uploaded to gs://{GCS_BUCKET_NAME}/{destination_blob_name}")

def check_file_freshness(blob_name, max_age_hours=24):
    """Checks if a file in GCS was updated within the specified time frame."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        return False
        
    blob.reload() 
    age = datetime.now(timezone.utc) - blob.updated
    return age < timedelta(hours=max_age_hours)

def fetch_sentinel5p(session):
    print("Initiating Sentinel-5P retrieval for the last 24 hours...")
    
    if check_file_freshness("sentinel-5p/metadata_log.csv", max_age_hours=24):
        print("Sentinel-5P data was already fetched within the last week. Skipping download.")
        return
        
    username = get_secret("copernicus_username")
    password = get_secret("copernicus_password")
    
    auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    auth_response = session.post(auth_url, data={
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password"
    })
    auth_response.raise_for_status()
    access_token = auth_response.json()["access_token"]
    
    class CDSE_Session(requests.Session):
        def rebuild_auth(self, prepared_request, response):
            super().rebuild_auth(prepared_request, response)
            prepared_request.headers["Authorization"] = f"Bearer {access_token}"

    dl_session = CDSE_Session()
    
    # Map your targets to S5P internal names. PM2.5/PM10 are excluded as S5P doesn't measure them.
    s5p_pollutants = {
        "ch4": "L2__CH4___",
        "no2": "L2__NO2___",
        "o3": "L2__O3____",
        "co": "L2__CO____",
        "so2": "L2__SO2___"
    }

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=24)
    date_filter = f"ContentDate/Start gt {start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
    
    downloaded_metadata = []

    for poll_key, poll_name in s5p_pollutants.items():
        print(f"\nSearching for {poll_key.upper()} ({poll_name}) over the last 24 hours...")
        
        query_url = (
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"$filter=Collection/Name eq 'SENTINEL-5P' and contains(Name, '{poll_name}') "
            f"and OData.CSC.Intersects(area=geography'SRID=4326;{MONTREAL_POLYGON}') "
            f"and {date_filter}&$orderby=ContentDate/Start desc"
        )
        
        search_response = session.get(query_url)
        search_response.raise_for_status()
        products = search_response.json().get("value", [])
        
        if not products:
            print(f"No {poll_key.upper()} products found in this timeframe.")
            continue

        for product in products:
            product_id = product["Id"]
            product_name = product["Name"]
            capture_date = product["ContentDate"]["Start"]
            print(f" -> Found: {product_name} | Date: {capture_date}")
            
            download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
            is_direct_nc = product_name.endswith('.nc')
            file_suffix = ".nc" if is_direct_nc else ".zip"

            with tempfile.NamedTemporaryFile(suffix=file_suffix, delete=False) as tmp_file:
                with dl_session.get(download_url, stream=True, allow_redirects=True) as r:
                    if r.status_code == 202:
                        print("    [Offline in Long Term Archive. Skipping.]")
                        os.remove(tmp_file.name)
                        continue
                        
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=8192):
                        tmp_file.write(chunk)
                tmp_file_path = tmp_file.name

            gcs_destination = f"sentinel-5p/{poll_key}/{product_name.replace('.zip', '.nc')}"

            if is_direct_nc:
                upload_to_gcs(tmp_file_path, gcs_destination)
            else:
                with zipfile.ZipFile(tmp_file_path, 'r') as z:
                    for filename in z.namelist():
                        if filename.endswith(".nc"):
                            temp_dir = tempfile.mkdtemp()
                            extracted_path = z.extract(filename, temp_dir)
                            upload_to_gcs(extracted_path, gcs_destination)
                            shutil.rmtree(temp_dir)
                            break
            
            os.remove(tmp_file_path)
            
            # Log successful download
            downloaded_metadata.append({
                "pollutant": poll_key,
                "capture_date": capture_date,
                "product_id": product_id,
                "gcs_path": gcs_destination
            })

    # Save tracking CSV locally and to GCP
    if downloaded_metadata:
        df_meta = pd.DataFrame(downloaded_metadata)
        local_csv = "data/sentinel5p_24h_metadata.csv"
        df_meta.to_csv(local_csv, index=False)
        upload_to_gcs(local_csv, "sentinel-5p/metadata_log.csv")
        print(f"\nFinished S5P. Downloaded {len(downloaded_metadata)} files. Metadata saved to {local_csv}.")

def fetch_openaq(session):
    print("\n--- Starting OpenAQ Retrieval ---")
    
    if check_file_freshness("openaq/latest_measurements.csv", max_age_hours=24):
        print(">> Status: GCS file is fresh (under 24 hours). Skipping download.")
        return

    openaq_key = os.getenv("OPENAQ_API_KEY")
    client = OpenAQ(api_key=openaq_key)
    
    MONTREAL_BBOX = (-73.97, 45.41, -73.47, 45.71)
    TARGET_POLLUTANTS = ["ch4", "pm25", "pm10", "no2", "o3", "co", "so2"]
    measurements_data = []

    try:
        print(f">> Querying locations in BBox: {MONTREAL_BBOX}")
        response = client.locations.list(bbox=MONTREAL_BBOX)
        locations_data = response.dict().get('results', [])
        
        if not locations_data:
            print(">> Warning: No active locations found in Montreal BBox.")
            return
            
        print(f">> Success: Found {len(locations_data)} locations.")

        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=24)

        for loc_idx, loc in enumerate(locations_data, 1):
            loc_name = loc.get("name", "Unknown Location")
            sensors = loc.get("sensors", [])
            print(f"   [{loc_idx}/{len(locations_data)}] Processing Location: {loc_name}")

            for sensor in sensors:
                param_name = sensor.get("parameter", {}).get("name")
                
                if param_name in TARGET_POLLUTANTS:
                    sensor_id = sensor["id"]
                    time.sleep(1.1) # Respect API limits
                    
                    try:
                        meas_response = client.measurements.list(
                            sensors_id=sensor_id,
                            datetime_from=start_time,
                            datetime_to=now,
                            limit=1000
                        )
                        results = meas_response.dict().get("results", [])
                        print(f"      - Sensor {sensor_id} ({param_name}): Found {len(results)} readings")
                        
                        for r in results:
                            period = r.get("period", {})
                            dt_val = period.get("datetimeTo") or period.get("datetime_to")
                            utc_time = dt_val.get("utc") if isinstance(dt_val, dict) else dt_val

                            if utc_time:
                                measurements_data.append({
                                    "location": loc_name,
                                    "parameter": param_name,
                                    "value": r["value"],
                                    "unit": sensor["parameter"].get("units", "unknown"),
                                    "lat": loc.get("coordinates", {}).get("latitude"),
                                    "lon": loc.get("coordinates", {}).get("longitude"),
                                    "utc_time": str(utc_time)
                                })
                    except Exception as e:
                        print(f"      ! Error fetching sensor {sensor_id}: {e}")

        print(f"\n>> Final Tally: {len(measurements_data)} total measurements collected.")

        if measurements_data:
            # Convert to Pandas DataFrame for CSV export
            df = pd.DataFrame(measurements_data)
            local_csv_path = "data/openaq_24hours_local.csv"
            df.to_csv(local_csv_path, index=False)
            print(f">> Saved locally to {local_csv_path}")
            
            # Upload the CSV directly to GCP
            upload_to_gcs(local_csv_path, "openaq/latest_measurements.csv")
            print(">> Uploaded to GCS.")
        else:
            print(">> Status: No relevant measurements found. Skipping GCS upload.")

    finally:
        print("--- OpenAQ Session Closed ---\n")

if __name__ == "__main__":
    http_session = get_http_session()
    fetch_sentinel5p(http_session)
    fetch_openaq(http_session)