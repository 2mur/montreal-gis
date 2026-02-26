# Montreal GIS Methane Project

An automated geospatial data pipeline deployed on GCP to ingest, process, and analyze methane emissions over Montreal. The system compares Sentinel-5P satellite imagery against OpenAQ terrestrial sensors to detect localized emission anomalies using an Isolation Forest model.

## Architecture & Tech Stack
* **Data Sources:** Copernicus Data Space Ecosystem (Sentinel-5P NetCDF), OpenAQ (JSON)
* **Storage:** Google Cloud Storage (Raw data landing zone), PostGIS (Processed spatial geometries)
* **Processing:** Python (GDAL, Rasterio, GeoPandas, Xarray, Shapely)
* **Transformation:** dbt (Spatial joins via `ST_Contains` and variance calculations)
* **Machine Learning:** Isolation Forest (scikit-learn) tracked via ClearML
* **Orchestration & Deployment:** Airflow, Docker, GCP Artifact Registry, Cloud Run Jobs

## Key Infrastructure Fixes Implemented
This project incorporates several critical fixes for common GIS and cloud deployment issues:
* **GDAL/Rasterio Compilation:** The `Dockerfile` uses a multi-stage approach, installing core C-libraries (`libgdal-dev`, `gdal-bin`, `gcc`) before installing Python geospatial requirements to prevent build crashes.
* **GCP Cloud Build Compatibility:** `Dockerfile` is explicitly excluded from `.gitignore` so that GCP can successfully build the image remotely via Artifact Registry.
* **Secure Authentication:** Copernicus API credentials are not hardcoded. They are fetched at runtime using **Google Cloud Secret Manager**.
* **Resilient Ingestion:** The `ingest.py` script utilizes exponential backoff (`urllib3` Retry) to handle Copernicus and OpenAQ gateway timeouts and rate limits (HTTP 429/50x).
* **Safe Zip Extraction:** Sentinel-5P `.zip` archives containing nested `.SAFE` directories are extracted and cleaned up recursively using `shutil.rmtree` to prevent `OSError` crashes in Cloud Run's ephemeral memory.
* **OpenAQ Rate Limiting:** Included an `X-API-Key` header fallback for OpenAQ to bypass strict unauthenticated IP blocking.
* **Local GCP Authentication:** Development uses a dedicated local service account (`gcp-key.json`) mounted as a read-only Docker volume, allowing seamless local testing of Secret Manager and Cloud Storage interactions.

## Prerequisites
* Docker Desktop and WSL2/PowerShell environment.
* Google Cloud CLI (`gcloud`) installed and authenticated.
* A registered account on the Copernicus Data Space Ecosystem.
* An OpenAQ API Key (Free tier).

## Local Setup & Configuration

### 1. Repository Initialization
Clone the repository and ensure your `.gitignore` is set up correctly.
**Crucial:** Do not track `data/`, `.env`, or `gcp-key.json`. Ensure `Dockerfile` is *not* in the `.gitignore`.

### 2. GCP Service Account & Secrets (PowerShell)
Create a dedicated service account for local testing and set up your Copernicus credentials in Secret Manager:

```powershell
$PROJECT_ID = "YOUR_EXACT_PROJECT_ID" 
$SA_EMAIL = "montreal-gis-local@$PROJECT_ID.iam.gserviceaccount.com"

# Create Service Account
gcloud iam service-accounts create montreal-gis-local --display-name="Montreal GIS Local Testing"
Start-Sleep -Seconds 10 # Wait for IAM propagation

# Grant Roles
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/secretmanager.secretAccessor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/storage.objectAdmin"

# Generate Local Key File (Keep this gitignored!)
gcloud iam service-accounts keys create gcp-key.json --iam-account=$SA_EMAIL

# Create Secrets
gcloud secrets create copernicus_username --replication-policy="automatic"
gcloud secrets create copernicus_password --replication-policy="automatic"
echo -n "your_email" | gcloud secrets versions add copernicus_username --data-file=-
echo -n "your_password" | gcloud secrets versions add copernicus_password --data-file=-