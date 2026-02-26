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

def check_file_freshness(blob_name, max_age_hours=168):
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
    print("Initiating Sentinel-5P retrieval...")
    
    # 168 hours = 1 week
    if check_file_freshness("sentinel-5p/latest.nc", max_age_hours=168):
        print("Sentinel-5P data was already fetched within the last week. Skipping download.")
        return
        
    username = get_secret("copernicus_username")
    password = get_secret("copernicus_password")
    
    auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    auth_data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    
    auth_response = session.post(auth_url, data=auth_data)
    auth_response.raise_for_status()
    access_token = auth_response.json()["access_token"]
    
    # The OData query naturally returns the latest available file regardless of date
    query_url = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
        "$filter=Collection/Name eq 'SENTINEL-5P' and contains(Name, 'L2__CH4') "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{MONTREAL_POLYGON}')"
        "&$top=1&$orderby=ContentDate/Start desc"
    )
    
    search_response = session.get(query_url)
    search_response.raise_for_status()
    products = search_response.json().get("value", [])
    
    if not products:
        print("No Sentinel-5P products found.")
        return
        
    product_id = products[0]["Id"]
    product_name = products[0]["Name"]
    print(f"Found product: {product_name} (ID: {product_id})")

    download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    
    class CDSE_Session(requests.Session):
        def rebuild_auth(self, prepared_request, response):
            super().rebuild_auth(prepared_request, response)
            prepared_request.headers["Authorization"] = f"Bearer {access_token}"

    dl_session = CDSE_Session()
    is_direct_nc = product_name.endswith('.nc')
    file_suffix = ".nc" if is_direct_nc else ".zip"

    with tempfile.NamedTemporaryFile(suffix=file_suffix, delete=False) as tmp_file:
        with dl_session.get(download_url, stream=True, allow_redirects=True) as r:
            if r.status_code == 202:
                print("Product is offline in the Long Term Archive. Try again later.")
                os.remove(tmp_file.name)
                return
                
            r.raise_for_status()
            
            content_type = r.headers.get('Content-Type', '')
            if 'text/html' in content_type or 'application/json' in content_type:
                error_text = r.text[:500]
                os.remove(tmp_file.name)
                raise ValueError(f"Expected data file, but API returned {content_type}. Snippet: {error_text}")
            
            for chunk in r.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
        tmp_file_path = tmp_file.name

    if is_direct_nc:
        upload_to_gcs(tmp_file_path, "sentinel-5p/latest.nc")
    else:
        print("Extracting NetCDF from archive...")
        with zipfile.ZipFile(tmp_file_path, 'r') as z:
            for filename in z.namelist():
                if filename.endswith(".nc"):
                    temp_dir = tempfile.mkdtemp()
                    extracted_path = z.extract(filename, temp_dir)
                    upload_to_gcs(extracted_path, "sentinel-5p/latest.nc")
                    shutil.rmtree(temp_dir)
                    break
                    
    os.remove(tmp_file_path)

def fetch_openaq(session):
    print("\n--- Starting OpenAQ Retrieval (SDK v3) ---")
    
    if check_file_freshness("openaq/latest_measurements.json", max_age_hours=168):
        print(">> Status: GCS file is fresh (under 1 week old). Skipping download.")
        return

    openaq_key = os.getenv("OPENAQ_API_KEY")
    client = OpenAQ(api_key=openaq_key)
    
    # Montreal BBox: (min_lon, min_lat, max_lon, max_lat)
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
        seven_days_ago = now - timedelta(days=7)

        for loc_idx, loc in enumerate(locations_data, 1):
            loc_name = loc.get("name", "Unknown Location")
            sensors = loc.get("sensors", [])
            print(f"   [{loc_idx}/{len(locations_data)}] Processing Location: {loc_name} ({len(sensors)} sensors)")

            for sensor in sensors:
                param_name = sensor.get("parameter", {}).get("name")
                
                if param_name in TARGET_POLLUTANTS:
                    sensor_id = sensor["id"]
                    # Sleep to respect 60 requests/minute limit
                    time.sleep(1.1)
                    
                    try:
                        meas_response = client.measurements.list(
                            sensors_id=sensor_id,
                            datetime_from=seven_days_ago,
                            datetime_to=now,
                            limit=1000
                        )
                        res_dict = meas_response.dict()
                        results = res_dict.get("results", [])
                        
                        print(f"      - Sensor {sensor_id} ({param_name}): Found {len(results)} readings")
                        
                        for r in results:
                            # Robust timestamp extraction
                            period = r.get("period", {})
                            dt_val = period.get("datetimeTo") or period.get("datetime_to")
                            
                            # Handle if dt_val is a dict with 'utc' or a raw string/datetime
                            utc_time = None
                            if isinstance(dt_val, dict):
                                utc_time = dt_val.get("utc")
                            else:
                                utc_time = dt_val

                            if utc_time:
                                measurements_data.append({
                                    "location": loc_name,
                                    "parameter": param_name,
                                    "value": r["value"],
                                    "unit": sensor["parameter"].get("units", "unknown"),
                                    "coordinates": loc.get("coordinates"),
                                    "date": {"utc": str(utc_time)}
                                })
                    except Exception as e:
                        print(f"      ! Error fetching sensor {sensor_id}: {e}")

        print(f"\n>> Final Tally: {len(measurements_data)} total measurements collected.")

        if measurements_data:
            with tempfile.NamedTemporaryFile(mode='w', suffix=".json", delete=False) as tmp_json:
                json.dump({"results": measurements_data}, tmp_json)
                tmp_json_path = tmp_json.name
            
            upload_to_gcs(tmp_json_path, "openaq/latest_measurements.json")
            os.remove(tmp_json_path)
        else:
            print(">> Status: No relevant measurements found. Skipping GCS upload.")

    finally:
        client.close()
        print("--- OpenAQ Session Closed ---\n")

if __name__ == "__main__":
    http_session = get_http_session()
    fetch_sentinel5p(http_session)
    fetch_openaq(http_session)