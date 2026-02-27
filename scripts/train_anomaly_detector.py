import os
import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.neighbors import LocalOutlierFactor
from clearml import Task
from google.cloud import storage
import google.auth

_, auth_project = google.auth.default()
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", auth_project)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def train_models():
    task = Task.init(
        project_name="Montreal_GIS_AirQuality", 
        task_name="Multi_Pollutant_Anomaly_Detection"
    )
    
    db_user = os.getenv("DB_USER", "gis_user")
    db_pass = os.getenv("DB_PASS", "gis_pass")
    db_host = os.getenv("DB_HOST", "postgis") 
    db_name = os.getenv("DB_NAME", "montreal_air_quality")
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    
    engine = create_engine(db_url)
    query = "SELECT * FROM stg_pollutant_comparison"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No paired data available in stg_pollutant_comparison.")
        return

    pollutants = df['sensor_parameter'].unique()
    print(f"Detected pollutants for analysis: {pollutants}")

    logger = task.get_logger()
    contamination_rate = 0.05 
    
    storage_client = storage.Client(project=PROJECT_ID) if PROJECT_ID and GCS_BUCKET_NAME else None

    # List to hold the dataframes with their new prediction columns
    processed_dfs = []

    for pollutant in pollutants:
        print(f"\n--- Analyzing Anomalies for: {pollutant.upper()} ---")
        pollutant_df = df[df['sensor_parameter'] == pollutant].copy()
        
        if len(pollutant_df) < 10:
            print(f"Skipping {pollutant}: Not enough data points.")
            continue

        X = pollutant_df[['sat_z_score', 'sen_z_score']]

        models = {
            "Isolation_Forest": IsolationForest(contamination=contamination_rate, random_state=42),
            "One_Class_SVM": OneClassSVM(nu=contamination_rate),
            "Local_Outlier_Factor": LocalOutlierFactor(contamination=contamination_rate)
        }

        for model_name, model in models.items():
            preds = model.fit_predict(X)
            num_anomalies = (preds == -1).sum()
            
            logger.report_scalar(
                title=f"Anomalies Found: {pollutant.upper()}", 
                series=model_name, value=num_anomalies, iteration=1
            )
            print(f"   - {model_name}: {num_anomalies} anomalies detected.")
            
            # Save the predictions to the dataframe (-1 is anomaly, 1 is normal)
            if model_name == "Isolation_Forest":
                pollutant_df['is_anomaly'] = preds == -1
                
                # We only save the Isolation Forest joblib for the production pipeline
                if storage_client:
                    model_filename = f"{pollutant}_isolation_forest.joblib"
                    joblib.dump(model, model_filename)
                    blob = storage_client.bucket(GCS_BUCKET_NAME).blob(f"models/anomaly_detection/{model_filename}")
                    blob.upload_from_filename(model_filename)
                    os.remove(model_filename)
                    
            elif model_name == "One_Class_SVM":
                pollutant_df['is_anomaly_svm'] = preds == -1
                
            elif model_name == "Local_Outlier_Factor":
                pollutant_df['is_anomaly_lof'] = preds == -1

        processed_dfs.append(pollutant_df)

    print("\nTraining complete. Check your ClearML dashboard for results.")

    # Combine all processed data and save locally
    if processed_dfs:
        final_results_df = pd.concat(processed_dfs, ignore_index=True)
        
        # Drop complex geometries before saving to CSV
        if 'sensor_location' in final_results_df.columns:
            final_results_df = final_results_df.drop(columns=['sensor_location', 'satellite_footprint'])
            
        os.makedirs("/app/data", exist_ok=True)
        local_csv_path = "/app/data/anomaly_predictions.csv"
        final_results_df.to_csv(local_csv_path, index=False)
        print(f"Saved {len(final_results_df)} records with anomaly flags to {local_csv_path}")

if __name__ == "__main__":
    train_models()