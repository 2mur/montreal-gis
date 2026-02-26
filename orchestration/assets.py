import subprocess
from dagster import asset, AssetExecutionContext

@asset(group_name="ingestion")
def raw_data_files(context: AssetExecutionContext):
    """Ingests data from GCP and OpenAQ SDK."""
    context.log.info("Starting ingestion...")
    subprocess.run(["python", "scripts/ingest.py"], check=True)

@asset(deps=[raw_data_files], group_name="spatial")
def postgis_tables(context: AssetExecutionContext):
    """Processes files into PostGIS spatial tables."""
    subprocess.run(["python", "scripts/process_spatial.py"], check=True)

@asset(deps=[postgis_tables], group_name="analytics")
def dbt_tables(context: AssetExecutionContext):
    """Runs dbt transformations."""
    # Note: Ensure your dbt_project is in the same container path
    subprocess.run(["dbt", "run", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"], check=True)

@asset(deps=[dbt_tables], group_name="ml")
def clearml_results(context: AssetExecutionContext):
    """Trains ML models and logs to ClearML."""
    subprocess.run(["python", "scripts/train_anomaly_detector.py"], check=True)

@asset(deps=[clearml_results], group_name="viz")
def anomaly_map_html(context: AssetExecutionContext):
    """Generates the final Folium HTML map and uploads to GCS."""
    subprocess.run(["python", "scripts/visualize_map.py"], check=True)