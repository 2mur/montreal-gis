# Montreal GIS Air Quality Project

An automated geospatial data pipeline deployed on Google Cloud Platform (GCP) to ingest, process, and analyze multi-pollutant atmospheric data over Montreal. The system compares Sentinel-5P satellite imagery against OpenAQ terrestrial sensors to detect localized emission anomalies (CH4, NO2, O3, CO, SO2) using an Isolation Forest model and visualizes the results on an interactive map.

## Architecture & Tech Stack

* **Data Sources:** Copernicus Data Space Ecosystem (Sentinel-5P NetCDF), OpenAQ (CSV)
* **Storage:** Google Cloud Storage (Raw data landing zone, ML model registry, HTML dashboard hosting), PostGIS (Processed spatial geometries)
* **Processing:** Python (GeoPandas, Xarray, Shapely, SQLAlchemy)
* **Transformation:** dbt (Spatial joins via `ST_Contains`, window functions for Z-score normalization)
* **Machine Learning:** Isolation Forest, OneClassSVM, LocalOutlierFactor (scikit-learn) tracked via ClearML
* **Visualization:** Folium (Leaflet), Plotly.js (Interactive time-series popups), GeoJSON layer rendering
* **Orchestration & Deployment:** Docker, GCP Artifact Registry, Cloud Run Jobs

## Key Infrastructure & Data Upgrades Implemented

This project incorporates several critical fixes for spatial data handling and cloud deployment:

* **Multi-Pollutant Z-Score Normalization:** Directly comparing satellite column density to ground-level parts-per-million (PPM) is scientifically invalid. The dbt pipeline uses PostgreSQL window functions to calculate Z-scores for each dataset partitioned by pollutant, allowing machine learning models to analyze standard deviations (`sat_z_score` vs `sen_z_score`) rather than mismatched raw units.
* **Accurate Spatial Buffering (CRS Projection):** Sentinel-5P pixels are projected into Montreal's local metric coordinate system (UTM Zone 18N / EPSG:32618) before applying a 2500m square buffer (`cap_style=3`), ensuring the physical footprint is accurate before projecting back to global degrees (EPSG:4326) for PostGIS ingestion.
* **Browser-Side Rendering Optimization:** Heavy server-side image generation (`matplotlib`) was stripped out. Satellite swaths are dynamically rendered as native GeoJSON polygons, and time-series charts are generated client-side using injected Plotly.js code, drastically reducing Docker image size and processing time.
* **Radial Jitter for Spatial Overlap:** OpenAQ stations often measure multiple gases from the exact same coordinate. The dashboard script applies a mathematical radial offset algorithm (spreading points at 0°, 90°, 180°, etc.) to prevent interactive map markers from completely overlapping.
* **Robust GCP Authentication:** Uses `google.auth.default()` for seamless authentication across local Docker environments (via volume-mounted `gcp-key.json`) and native GCP deployment, seamlessly pushing `.joblib` model artifacts and dashboard HTML to Cloud Storage.
* **Memory-Safe Processing:** Ephemeral NetCDF files (10-15GB uncompressed) are downloaded using `tempfile`, processed via `xarray`, and strictly purged in `finally` blocks to prevent Cloud Run instances from crashing due to out-of-memory errors.

## Prerequisites

* Docker Desktop and WSL2/PowerShell environment.
* Google Cloud CLI (`gcloud`) installed and authenticated.
* A registered account on the Copernicus Data Space Ecosystem.
* An OpenAQ API Key.
* ClearML account and API credentials.

```
