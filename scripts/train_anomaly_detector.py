import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.neighbors import LocalOutlierFactor
from clearml import Task

def train_models():
    # Initialize ClearML task
    task = Task.init(project_name="Montreal_GIS_AirQuality", task_name="Multi_Pollutant_Anomaly_Detection")
    
    # Database Connection
    db_user = os.getenv("DB_USER", "gis_user")
    db_pass = os.getenv("DB_PASS", "gis_pass")
    db_host = os.getenv("DB_HOST", "postgis") 
    db_name = os.getenv("DB_NAME", "montreal_methane")
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    
    engine = create_engine(db_url)
    # We fetch the unified view created by dbt
    query = "SELECT * FROM stg_methane_comparison"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No paired data available in stg_methane_comparison. Ensure dbt run was successful.")
        return

    # Identify which pollutants we actually have data for
    pollutants = df['sensor_parameter'].unique()
    print(f"Detected pollutants for analysis: {pollutants}")

    logger = task.get_logger()
    contamination_rate = 0.05 # Assume 5% of data points are anomalies

    for pollutant in pollutants:
        print(f"\n--- Analyzing Anomalies for: {pollutant} ---")
        pollutant_df = df[df['sensor_parameter'] == pollutant].copy()
        
        if len(pollutant_df) < 10:
            print(f"Skipping {pollutant}: Not enough data points (need at least 10).")
            continue

        # Feature: The value of the ground sensor reading
        X = pollutant_df[['sensor_ch4']]

        models = {
            "Isolation_Forest": IsolationForest(contamination=contamination_rate, random_state=42),
            "One_Class_SVM": OneClassSVM(nu=contamination_rate),
            "Local_Outlier_Factor": LocalOutlierFactor(contamination=contamination_rate)
        }

        for model_name, model in models.items():
            # fit_predict returns -1 for anomalies
            preds = model.fit_predict(X)
            num_anomalies = (preds == -1).sum()
            
            # Log the number of anomalies found for this pollutant
            logger.report_scalar(
                title=f"Anomalies Found: {pollutant}", 
                series=model_name, 
                value=num_anomalies, 
                iteration=1
            )
            
            print(f"   - {model_name}: {num_anomalies} anomalies detected.")

    print("\nTraining complete. Check your ClearML dashboard for results.")

if __name__ == "__main__":
    train_models()