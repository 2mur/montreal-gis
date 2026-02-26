import os
import geopandas as gpd
import folium
from sqlalchemy import create_engine
from google.cloud import storage

def create_anomaly_map():
    # 1. Connect and Fetch Data
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)
    
    query = "SELECT * FROM stg_methane_comparison"
    gdf = gpd.read_postgis(query, engine, geom_col='sensor_location', crs="EPSG:4326")
    
    # 2. Calculate Stats for the Sidebar
    total_readings = len(gdf)
    anomalies_df = gdf[gdf['ch4_variance'] > 0.5]
    total_anomalies = len(anomalies_df)
    
    stats_html = ""
    for param in gdf['sensor_parameter'].unique():
        param_anomalies = len(anomalies_df[anomalies_df['sensor_parameter'] == param])
        stats_html += f"""
        <div class="stat-box">
            <h3>{param.upper()}</h3>
            <p>Detected Anomalies: {param_anomalies}</p>
        </div>
        """

    # 3. Create the Cyberpunk Map
    m = folium.Map(location=[45.5017, -73.5673], zoom_start=11, tiles='cartodbdark_matter')

    for _, row in gdf.iterrows():
        is_anomaly = row.get('ch4_variance', 0) > 0.5
        # Cyberpunk neon orange for anomalies, neon cyan for normal
        color = '#ff5e00' if is_anomaly else '#00ffcc' 
        radius = 8 if is_anomaly else 4
        
        folium.CircleMarker(
            location=[row.sensor_location.y, row.sensor_location.x],
            radius=radius,
            color=color,
            weight=2 if is_anomaly else 1,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=f"Param: {row['sensor_parameter']}<br>Variance: {row['ch4_variance']:.4f}",
        ).add_to(m)

    # Sentinel-5P Bounding Box in dashed orange wireframe
    folium.Rectangle(
        bounds=[[45.41, -73.97], [45.71, -73.47]],
        color="#ff5e00",
        weight=2,
        fill=False,
        dash_array='5, 5'
    ).add_to(m)

    # 4. Build the Dashboard HTML Wrapper
    map_html = m.get_root().render()
    escaped_map_html = map_html.replace('"', '&quot;')

    dashboard_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Montreal Air Quality - Anomaly Dashboard</title>
        <style>
            body {{
                background-color: #0a0a0a;
                color: #00ffcc;
                font-family: 'Courier New', Courier, monospace;
                margin: 0;
                padding: 0;
                display: flex;
                height: 100vh;
                overflow: hidden;
            }}
            .sidebar {{
                width: 25%;
                padding: 20px;
                background-color: #111;
                border-right: 2px solid #ff5e00;
                box-shadow: 5px 0 15px rgba(255, 94, 0, 0.2);
                overflow-y: auto;
            }}
            .map-container {{
                width: 75%;
                height: 100%;
            }}
            h1 {{
                color: #ff5e00;
                text-transform: uppercase;
                border-bottom: 1px solid #ff5e00;
                padding-bottom: 10px;
                font-size: 1.5em;
            }}
            .summary {{
                font-size: 1.1em;
                margin-bottom: 30px;
                padding-bottom: 10px;
                border-bottom: 1px dashed #00ffcc;
            }}
            .stat-box {{
                border: 1px solid #00ffcc;
                padding: 15px;
                margin-bottom: 15px;
                background: rgba(0, 255, 204, 0.05);
                border-left: 4px solid #ff5e00;
            }}
            .stat-box h3 {{
                margin: 0 0 10px 0;
                color: #ff5e00;
            }}
            iframe {{
                width: 100%;
                height: 100%;
                border: none;
            }}
        </style>
    </head>
    <body>
        <div class="sidebar">
            <h1>System Diagnostics</h1>
            <div class="summary">
                <p>TOTAL SCANS: {total_readings}</p>
                <p style="color: #ff5e00;">ANOMALIES FOUND: {total_anomalies}</p>
            </div>
            {stats_html}
        </div>
        <div class="map-container">
            <iframe srcdoc="{escaped_map_html}"></iframe>
        </div>
    </body>
    </html>
    """

    # 5. Upload to Google Cloud Storage
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    
    if bucket_name and project_id:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("maps/montreal_anomalies_v2.html")
        blob.upload_from_string(dashboard_html, content_type="text/html")
        print(f"Dashboard uploaded to gs://{bucket_name}/maps/montreal_anomalies_v2.html")
    else:
        print("Missing GCP vars. Saving locally to /tmp/dashboard.html")
        with open("/tmp/dashboard.html", "w", encoding="utf-8") as f:
            f.write(dashboard_html)

if __name__ == "__main__":
    create_anomaly_map()