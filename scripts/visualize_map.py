import os
import math
import pandas as pd
import geopandas as gpd
import folium
import branca.colormap as cm
import xarray as xr
import numpy as np
from sqlalchemy import create_engine
from google.cloud import storage
import google.auth

# --- Visualization Configurations ---
POLLUTANT_COLORS = {
    "ch4": "#ff8c00", # Neon Orange
    "no2": "#00ff80", # Neon turquoise
    "o3":  "#b026ff", # Neon Purple
    "co":  "#00ffcc", # Neon Cyan
    "so2": "#ffff00"  # Neon Yellow
}

S5P_CONFIG = {
    "ch4": "methane_mixing_ratio_bias_corrected",
    "no2": "nitrogendioxide_tropospheric_column",
    "o3":  "ozone_total_vertical_column",
    "co":  "carbonmonoxide_total_column",
    "so2": "sulfurdioxide_total_vertical_column"
}

_, auth_project = google.auth.default()
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", auth_project)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def create_plotly_popup(group, parameter, color, sensor_name):
    group = group.sort_values('sensor_time')
    
    times = group['sensor_time'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    values = group['ground_value'].tolist()
    anomalies = group['is_anomaly'].tolist()
    
    norm_times = [t for t, a in zip(times, anomalies) if not a]
    norm_vals = [v for v, a in zip(values, anomalies) if not a]
    
    anom_times = [t for t, a in zip(times, anomalies) if a]
    anom_vals = [v for v, a in zip(values, anomalies) if a]
    
    mean_val = group['ground_value'].mean()
    std_val = group['ground_value'].std()
    boundary = mean_val + (2 * std_val) if pd.notnull(std_val) and std_val > 0 else mean_val

    safe_sensor_name = str(sensor_name).replace("'", "\\'")

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script></head>
    <body style="background-color:#111; margin:0;">
        <div id="loading" style="color:#00ffcc; text-align:center; padding-top:100px; font-family:'Courier New', monospace;">[LOADING DATA...]</div>
        <div id="plot" style="width:380px; height:260px; display:none;"></div>
        <script>
            var traceLine = {{ x: {times}, y: {values}, mode: 'lines', line: {{color: '{color}', width: 1}}, opacity: 0.5, name: 'Trend' }};
            var traceNormal = {{ x: {norm_times}, y: {norm_vals}, mode: 'markers', marker: {{color: '{color}', size: 6}}, name: 'Normal' }};
            var traceAnomaly = {{ x: {anom_times}, y: {anom_vals}, mode: 'markers', marker: {{color: 'red', size: 12, line: {{color: 'white', width: 1}}}}, name: 'Anomaly' }};
            var traceBoundary = {{ x: ['{times[0]}', '{times[-1]}'], y: [{boundary}, {boundary}], mode: 'lines', line: {{color: 'red', dash: 'dash', width: 1}}, name: '2σ Threshold' }};
            
            var layout = {{
                paper_bgcolor: '#111', plot_bgcolor: '#111',
                margin: {{l: 40, r: 10, t: 40, b: 40}},
                title: {{text: '<b>{safe_sensor_name}</b><br>{parameter.upper()} 24h Trend', font: {{color: '{color}', size: 12}}}},
                xaxis: {{tickfont: {{color: '#888', size: 10}}, gridcolor: '#333'}},
                yaxis: {{tickfont: {{color: '#888', size: 10}}, gridcolor: '#333'}},
                showlegend: false
            }};
            Plotly.newPlot('plot', [traceLine, traceNormal, traceAnomaly, traceBoundary], layout, {{displayModeBar: false}}).then(function() {{
                document.getElementById('loading').style.display = 'none';
                document.getElementById('plot').style.display = 'block';
            }});
        </script>
    </body>
    </html>
    """
    return html

def create_anomaly_map():
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME', 'montreal_air_quality')}"
    engine = create_engine(db_url)
    
    query = """
        SELECT 
            geom AS sensor_location,
            sensor_name,
            parameter AS sensor_parameter,
            timestamp AS sensor_time,
            measurement_value AS ground_value
        FROM openaq_data
    """
    gdf = gpd.read_postgis(query, engine, geom_col='sensor_location', crs="EPSG:4326")
    
    if gdf.empty:
        print("No data available to map.")
        return

    local_csv_path = "/app/data/anomaly_predictions.csv"
    if os.path.exists(local_csv_path):
        preds_df = pd.read_csv(local_csv_path)
        preds_df['sensor_time'] = pd.to_datetime(preds_df['sensor_time'])
        
        gdf = gdf.merge(
            preds_df[['sensor_time', 'sensor_parameter', 'ground_value', 'is_anomaly']], 
            on=['sensor_time', 'sensor_parameter', 'ground_value'], 
            how='left'
        )
        gdf['is_anomaly'] = gdf['is_anomaly'].fillna(False)
    else:
        gdf['is_anomaly'] = False

    gdf['lon'] = gdf.sensor_location.x
    gdf['lat'] = gdf.sensor_location.y

    # Removed basemap tiles. Canvas will be set to dark mode via CSS.
    # Add the Target BBox as the new "basemap" filled contour
    # ... existing code ...
    m = folium.Map(location=[45.5017, -73.5673], zoom_start=11, tiles=None)

    dark_mode_css = """
    <style>
        .leaflet-container { background-color: #0a0a0a !important; }
        .leaflet-popup-content-wrapper, .leaflet-popup-tip { background-color: #111 !important; color: #00ffcc !important; border: 1px solid #444; }
        .leaflet-control-layers-expanded { background-color: #111 !important; color: #00ffcc !important; border: 1px solid #ff5e00 !important; border-radius: 0 !important; }
        .leaflet-control-zoom a { background-color: #111 !important; color: #ff5e00 !important; border: 1px solid #444 !important; }
    </style>
    """
    m.get_root().html.add_child(folium.Element(dark_mode_css))

    # --- NEW: Load and Draw the Montreal Contour ---
    contour_path = "/app/data/montreal-zones.geojson"
    if os.path.exists(contour_path):
        contour_gdf = gpd.read_file(contour_path)
        
        # Draw the island with a cyberpunk wireframe aesthetic
        folium.GeoJson(
            contour_gdf,
            name="Montreal Island Contour",
            style_function=lambda feature: {
                'fillColor': '#111111',   # Dark grey interior
                'color': '#333333',       # Subtle grey border
                'weight': 1.5,
                'fillOpacity': 0.6
            }
        ).add_to(m)
    else:
        print(f"Warning: Contour file not found at {contour_path}. Map will be blank.")

    # Add the Target BBox as a dashed zone
    folium.Rectangle(
        bounds=[[45.41, -73.97], [45.71, -73.47]], 
        color="#ff5e00", 
        weight=1, 
        fill=False, 
        dash_array='5, 5',
        name="Target BBox Region"
    ).add_to(m)

    fg_dict = {}
    for param in gdf['sensor_parameter'].unique():
        p_lower = param.lower()
        fg_dict[p_lower] = folium.FeatureGroup(name=f"Ground: {p_lower.upper()}", show=True)
        m.add_child(fg_dict[p_lower])

    locations = gdf[['lon', 'lat']].drop_duplicates()

    for _, loc in locations.iterrows():
        base_lon, base_lat = loc['lon'], loc['lat']
        loc_data = gdf[(gdf['lon'] == base_lon) & (gdf['lat'] == base_lat)]
        
        unique_params = loc_data['sensor_parameter'].unique()
        n_params = len(unique_params)
        
        spread_radius = 0.003 if n_params > 1 else 0 

        for i, param in enumerate(unique_params):
            param_lower = param.lower()
            param_data = loc_data[loc_data['sensor_parameter'] == param]
            color = POLLUTANT_COLORS.get(param_lower, "#ffffff")
            sensor_name = param_data.iloc[0]['sensor_name']
            has_anomaly = param_data['is_anomaly'].any()
            
            angle_rad = math.radians(i * (360 / n_params)) if n_params > 1 else 0
            j_lon = base_lon + (spread_radius * math.cos(angle_rad))
            j_lat = base_lat + (spread_radius * math.sin(angle_rad))
            
            popup_html = create_plotly_popup(param_data, param_lower, color, sensor_name)
            iframe = folium.IFrame(html=popup_html, width=400, height=280)
            
            if has_anomaly:
                folium.CircleMarker(
                    [j_lat, j_lon], radius=18, color=None, fill=True, fill_color='red', fill_opacity=0.3,
                    popup=folium.Popup(iframe, max_width=500)
                ).add_to(fg_dict[param_lower])
                
                folium.CircleMarker(
                    [j_lat, j_lon], radius=10, color=None, fill=True, fill_color=color, fill_opacity=0.4, 
                    popup=folium.Popup(iframe, max_width=500)
                ).add_to(fg_dict[param_lower])
                
            else:
                folium.CircleMarker(
                    [j_lat, j_lon], radius=12, color=None, fill=True, fill_color=color, fill_opacity=0.3,
                    popup=folium.Popup(iframe, max_width=500)
                ).add_to(fg_dict[param_lower])
                
                folium.CircleMarker(
                    [j_lat, j_lon], radius=6, color=None, fill=True, fill_color=color, fill_opacity=0.3,
                    popup=folium.Popup(iframe, max_width=500)
                ).add_to(fg_dict[param_lower])
                

    storage_client = storage.Client(project=PROJECT_ID) if PROJECT_ID and GCS_BUCKET_NAME else None

    if storage_client:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        print("Processing NetCDF files into GeoJSON...")
        
        for pollutant, var_name in S5P_CONFIG.items():
            blobs = list(bucket.list_blobs(prefix=f"sentinel-5p/{pollutant}/"))
            if not blobs: continue
                
            latest_blob = max(blobs, key=lambda b: b.time_created)
            local_nc_path = f"/tmp/{pollutant}_latest.nc"
            latest_blob.download_to_filename(local_nc_path)
            
            try:
                ds = xr.open_dataset(local_nc_path, group='PRODUCT')
                
                if var_name not in ds and pollutant == "ch4":
                    var_name = "methane_mixing_ratio"
                    
                df = ds[[var_name, 'longitude', 'latitude']].isel(time=0).to_dataframe().reset_index()
                df = df.dropna(subset=[var_name, 'longitude', 'latitude'])
                
                df = df[
                    (df['longitude'] >= -73.97) & (df['longitude'] <= -73.47) &
                    (df['latitude'] >= 45.41) & (df['latitude'] <= 45.71)
                ]
                
                if df.empty: continue
                
                sat_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
                sat_gdf = sat_gdf.to_crs(epsg=32618)
                sat_gdf['geometry'] = sat_gdf.geometry.buffer(2500, cap_style=3)
                sat_gdf = sat_gdf.to_crs(epsg=4326)
                
                sat_gdf = sat_gdf[['geometry', var_name]].copy()
                
                color_hex = POLLUTANT_COLORS.get(pollutant, "#ffffff")
                colormap = cm.LinearColormap(['#111111', color_hex], vmin=sat_gdf[var_name].min(), vmax=sat_gdf[var_name].max())
                
                fg_sat = folium.FeatureGroup(name=f"Satellite: {pollutant.upper()}", show=False)
                
                folium.GeoJson(
                    sat_gdf,
                    style_function=lambda feature, cmap=colormap, var=var_name: {
                        'fillColor': cmap(feature['properties'][var]),
                        'color': cmap(feature['properties'][var]),
                        'weight': 1,
                        'fillOpacity': 0.3
                    },
                    tooltip=folium.GeoJsonTooltip(fields=[var_name], aliases=[f'{pollutant.upper()} Value:'])
                ).add_to(fg_sat)
                
                m.add_child(fg_sat)
                
            except Exception as e:
                print(f"Error processing {pollutant}: {e}")
            finally:
                if os.path.exists(local_nc_path):
                    os.remove(local_nc_path)

    folium.LayerControl(position='topright', collapsed=False).add_to(m)

    map_html = m.get_root().render()
    escaped_map_html = map_html.replace('"', '&quot;')

    total_readings = len(gdf)
    total_anomalies = len(gdf[gdf['is_anomaly']])
    
    stats_html = ""
    for param in gdf['sensor_parameter'].unique():
        param_anomalies = len(gdf[(gdf['sensor_parameter'] == param) & (gdf['is_anomaly'])])
        color = POLLUTANT_COLORS.get(param.lower(), "#ffffff")
        stats_html += f"""
        <div class="stat-box" style="border-left: 4px solid {color};">
            <h3 style="color: {color}; margin:0 0 5px 0;">{param.upper()}</h3>
            <p style="margin:0; font-size:0.9em;">Flagged Anomalies: {param_anomalies}</p>
        </div>
        """

    dashboard_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Montreal Air Quality - Anomaly Dashboard</title>
        <style>
            body {{ background-color: #0a0a0a; color: #00ffcc; font-family: 'Courier New', monospace; margin: 0; display: flex; height: 100vh; overflow: hidden; }}
            .sidebar {{ width: 25%; padding: 20px; background-color: #111; border-right: 2px solid #444; box-shadow: 5px 0 15px rgba(0,0,0, 0.5); overflow-y: auto; z-index: 1000; }}
            .map-container {{ width: 75%; height: 100%; position: relative; background-color: #0a0a0a; }}
            h1 {{ color: #fff; text-transform: uppercase; border-bottom: 1px solid #444; padding-bottom: 10px; font-size: 1.5em; margin-top:0; }}
            .summary {{ font-size: 1.1em; margin-bottom: 30px; padding-bottom: 10px; border-bottom: 1px dashed #444; }}
            .stat-box {{ border: 1px solid #333; padding: 10px; margin-bottom: 10px; background: rgba(255, 255, 255, 0.02); }}
            iframe {{ width: 100%; height: 100%; border: none; }}
            ::-webkit-scrollbar {{ width: 8px; }}
            ::-webkit-scrollbar-track {{ background: #111; }}
            ::-webkit-scrollbar-thumb {{ background: #444; }}
            ::-webkit-scrollbar-thumb:hover {{ background: #00ffcc; }}
        </style>
    </head>
    <body>
        <div class="sidebar">
            <h1>System Diagnostics</h1>
            <div class="summary">
                <p style="margin: 5px 0;">TOTAL SCANS: <span style="color:#fff">{total_readings}</span></p>
                <p style="margin: 5px 0;">TOTAL ANOMALIES: <span style="color:red">{total_anomalies}</span></p>
            </div>
            {stats_html}
        </div>
        <div class="map-container">
            <iframe srcdoc="{escaped_map_html}"></iframe>
        </div>
    </body>
    </html>
    """

    local_dir = "/app/data"
    os.makedirs(local_dir, exist_ok=True)
    filename = "montreal_anomalies_v5.html" 
    local_path = os.path.join(local_dir, filename)
    
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(dashboard_html)
    print(f"Dashboard saved locally to {local_path}")

    if storage_client:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob("maps/montreal_anomalies_latest.html")
        
        # Upload the HTML string
        blob.upload_from_string(dashboard_html, content_type="text/html")
        
        # Make this specific file publicly readable
        blob.make_public()
        
        print(f"Dashboard uploaded and made public!")
        print(f"Shareable Link: {blob.public_url}")
    else:
        print("Missing GCP variables. Skipped cloud upload.")

if __name__ == "__main__":
    create_anomaly_map()