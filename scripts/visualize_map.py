import os
import math
import json
import pandas as pd
import geopandas as gpd
import folium
import branca.colormap as cm
import xarray as xr
import numpy as np
from sqlalchemy import create_engine
from google.cloud import storage
import google.auth

pd.set_option('future.no_silent_downcasting', True)

# --- NGE Tactical Configurations ---
POLLUTANT_COLORS = {
    "ch4": "#FF4500", # Warning Orange
    "no2": "#53FF45", # MAGI Green
    "o3":  "#8A2BE2", # Eva-01 Purple
    "co":  "#00E5FF", # Tactical Cyan
    "so2": "#FFD700"  # Alert Yellow
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
    
    times = json.dumps(group['sensor_time'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist())
    ground_values = json.dumps([v if pd.notnull(v) else None for v in group['ground_value']])
    
    anomalies = group.get('is_anomaly', pd.Series([False]*len(group))).tolist()
    anom_times = json.dumps([t for t, a in zip(group['sensor_time'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(), anomalies) if a])
    anom_vals = json.dumps([v for v, a in zip(group['ground_value'].tolist(), anomalies) if a])

    y_min, y_max = group['ground_value'].min(), group['ground_value'].max()
    std_val = group['ground_value'].std()
    pad = (2 * std_val) if pd.notnull(std_val) and std_val > 0 else (y_max - y_min) * 0.1
    y_range = f"[{y_min - pad}, {y_max + pad}]"

    safe_sensor_name = str(sensor_name).replace("'", "\\'")

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="background-color:#050505; margin:0; overflow:hidden; font-family:'Roboto', monospace; border: 1px solid {color}; box-sizing: border-box;">
        <div style="background-color:{color}; color:#000; padding:2px 5px; font-weight:bold; font-size:12px;">TARGET LOCKED // {parameter.upper()}</div>
        <div id="plot" style="width:378px; height:218px;"></div>
        <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
        <script>
            var traceMain = {{ x: {times}, y: {ground_values}, mode: 'lines+markers', name: 'Sensor', line: {{color: '{color}', width: 1, shape: 'hv'}}, marker: {{color: '{color}', size: 4, symbol: 'square'}} }};
            var traceAnom = {{ x: {anom_times}, y: {anom_vals}, mode: 'markers', name: 'Anomaly', marker: {{color: '#FF4500', size: 10, symbol: 'cross', line: {{color: '#FF4500', width: 2}}}} }};
            
            var layout = {{
                paper_bgcolor: '#050505', plot_bgcolor: '#050505', margin: {{ l: 40, r: 10, t: 25, b: 30 }},
                title: {{text: 'ID: {safe_sensor_name}', font: {{color: '#888', size: 10}}}},
                xaxis: {{tickfont: {{color: '#53FF45', size: 9}}, gridcolor: '#1a1a1a', gridwidth: 1}}, 
                yaxis: {{range: {y_range}, tickfont: {{color: '#53FF45', size: 9}}, gridcolor: '#1a1a1a', gridwidth: 1, zeroline: true, zerolinewidth: 2, zerolinecolor: '#FF4500'}},
                showlegend: false
            }};
            Plotly.newPlot('plot', [traceMain, traceAnom], layout, {{displayModeBar: false}});
        </script>
    </body>
    </html>
    """
    return html

def create_anomaly_map():
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME', 'montreal_air_quality')}"
    engine = create_engine(db_url)
    
    query = "SELECT geom AS sensor_location, sensor_name, parameter AS sensor_parameter, timestamp AS sensor_time, measurement_value AS ground_value FROM openaq_data"
    gdf = gpd.read_postgis(query, engine, geom_col='sensor_location', crs="EPSG:4326")
    
    if gdf.empty:
        print("No data available to map.")
        return

    local_csv_path = "/app/data/anomaly_predictions.csv"
    if os.path.exists(local_csv_path):
        preds_df = pd.read_csv(local_csv_path)
        preds_df['sensor_time'] = pd.to_datetime(preds_df['sensor_time'])
        gdf = gdf.merge(preds_df, on=['sensor_time', 'sensor_parameter', 'ground_value'], how='left')
        gdf['is_anomaly'] = gdf.get('is_anomaly', pd.Series([False]*len(gdf))).fillna(False)
    else:
        gdf['is_anomaly'] = False

    gdf['lon'], gdf['lat'] = gdf.sensor_location.x, gdf.sensor_location.y

    m = folium.Map(location=[45.5017, -73.5673], zoom_start=12, tiles=None, max_bounds=True, min_zoom=11, maxBoundsViscosity=1.0)
    m.fit_bounds([[45.38, -74.00], [45.74, -73.44]]) 

    map_css = """
    <style>
        path.leaflet-interactive:focus { outline: none !important; }
        .leaflet-container *:focus { outline: none !important; }
        .leaflet-container { 
            background-color: #050505 !important; 
            background-image: linear-gradient(rgba(83, 255, 69, 0.15) 1px, transparent 1px), linear-gradient(90deg, rgba(83, 255, 69, 0.15) 1px, transparent 1px);
            background-size: 40px 40px;
        }
        .leaflet-popup-content-wrapper { background-color: transparent !important; border: none !important; box-shadow: 0 0 15px rgba(83,255,69,0.4); padding: 0 !important;}
        .leaflet-popup-tip { display: none !important; }
        .leaflet-popup-content { margin: 0 !important; }
    </style>
    """
    m.get_root().html.add_child(folium.Element(map_css))

    folium.Rectangle(
        bounds=[[45.41, -73.97], [45.71, -73.47]], color="#FF4500", weight=2, dash_array='10, 10',
        fill=True, fill_color="#000000", fill_opacity=0.85, name="TARGET REGION"
    ).add_to(m)

    contour_path = "/app/data/montreal-zones.geojson"
    if os.path.exists(contour_path):
        folium.GeoJson(
            gpd.read_file(contour_path), name="GEOGRAPHIC BNDRY",
            style_function=lambda x: {'fillColor': '#0a0a0a', 'color': '#53FF45', 'weight': 1, 'fillOpacity': 0.7, 'dashArray': '3,3'}
        ).add_to(m)

    fg_dict = {p.lower(): folium.FeatureGroup(name=f"SENSOR: {p.upper()}", show=True).add_to(m) for p in gdf['sensor_parameter'].unique()}

    # Helper function to generate fresh popup objects
    def get_popup(html_content):
        return folium.Popup(folium.IFrame(html=html_content, width=380, height=240), max_width=380)

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
            has_anomaly = param_data.get('is_anomaly', pd.Series([False])).any()
            
            angle_rad = math.radians(i * (360 / n_params)) if n_params > 1 else 0
            j_lon, j_lat = base_lon + (spread_radius * math.cos(angle_rad)), base_lat + (spread_radius * math.sin(angle_rad))
            
            popup_html = create_plotly_popup(param_data, param_lower, color, sensor_name)
            
            # The Fix: Call get_popup() independently for every marker so Folium generates unique JS IDs
            if has_anomaly:
                folium.CircleMarker([j_lat, j_lon], radius=18, fill=True, fill_color='#FF4500', color='#FF4500', weight=1, fill_opacity=0.2, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])
                folium.CircleMarker([j_lat, j_lon], radius=10, fill=True, fill_color=color, color=color, weight=2, fill_opacity=0.4, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])
                folium.CircleMarker([j_lat, j_lon], radius=3, fill=True, fill_color='#fff', color='#fff', weight=1, fill_opacity=1, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])
            else:
                folium.CircleMarker([j_lat, j_lon], radius=12, fill=True, fill_color=color, color=color, weight=1, fill_opacity=0.1, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])
                folium.CircleMarker([j_lat, j_lon], radius=6, fill=True, fill_color=color, color=color, weight=1, fill_opacity=0.3, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])
                folium.CircleMarker([j_lat, j_lon], radius=2, fill=True, fill_color='#fff', color='#fff', weight=1, fill_opacity=0.8, popup=get_popup(popup_html)).add_to(fg_dict[param_lower])

    storage_client = storage.Client(project=PROJECT_ID) if PROJECT_ID and GCS_BUCKET_NAME else None
    if storage_client:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        for pollutant, var_name in S5P_CONFIG.items():
            blobs = list(bucket.list_blobs(prefix=f"sentinel-5p/{pollutant}/"))
            if not blobs: continue
            local_nc_path = f"/tmp/{pollutant}_latest.nc"
            max(blobs, key=lambda b: b.time_created).download_to_filename(local_nc_path)
            try:
                ds = xr.open_dataset(local_nc_path, group='PRODUCT')
                if var_name not in ds and pollutant == "ch4": var_name = "methane_mixing_ratio"
                df = ds[[var_name, 'longitude', 'latitude']].isel(time=0).to_dataframe().reset_index().dropna(subset=[var_name, 'longitude', 'latitude'])
                df = df[(df['longitude'] >= -73.97) & (df['longitude'] <= -73.47) & (df['latitude'] >= 45.41) & (df['latitude'] <= 45.71)]
                if df.empty: continue
                
                sat_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326").to_crs(epsg=32618)
                sat_gdf['geometry'] = sat_gdf.geometry.buffer(2500, cap_style=3)
                sat_gdf = sat_gdf.to_crs(epsg=4326)[['geometry', var_name]].copy()
                
                color_hex = POLLUTANT_COLORS.get(pollutant, "#ffffff")
                colormap = cm.LinearColormap(['#111111', color_hex], vmin=sat_gdf[var_name].min(), vmax=sat_gdf[var_name].max())
                
                fg_sat = folium.FeatureGroup(name=f"ORBITAL: {pollutant.upper()}", show=False)
                folium.GeoJson(sat_gdf, style_function=lambda f, cmap=colormap, var=var_name: {'fillColor': cmap(f['properties'][var]), 'color': cmap(f['properties'][var]), 'weight': 1, 'fillOpacity': 0.35}, tooltip=folium.GeoJsonTooltip(fields=[var_name], aliases=[f'{pollutant.upper()}:'])).add_to(fg_sat)
                m.add_child(fg_sat)
            except Exception as e: pass
            finally: 
                if os.path.exists(local_nc_path): os.remove(local_nc_path)

    folium.LayerControl(position='topleft', collapsed=False).add_to(m)
    map_html = m.get_root().render()

    stats_html = ""
    for param in gdf['sensor_parameter'].unique():
        param_data = gdf[gdf['sensor_parameter'] == param]
        color = POLLUTANT_COLORS.get(param.lower(), "#ffffff")
        tot_if = int(pd.Series(param_data.get('is_anomaly', [False]*len(param_data))).sum())
        tot_svm = int(pd.Series(param_data.get('is_anomaly_svm', [False]*len(param_data))).sum())
        tot_lof = int(pd.Series(param_data.get('is_anomaly_lof', [False]*len(param_data))).sum())
        
        stats_html += f"""
        <div style="border: 1px solid {color}; padding: 10px; margin-bottom: 10px; background: rgba(0,0,0,0.8); position: relative;">
            <div style="position: absolute; top:0; left:0; width: 100%; height: 3px; background-color: {color};"></div>
            <h4 style="color: {color}; margin: 5px 0 10px 0; font-weight: bold; letter-spacing: 1px;">{param.upper()} // ANOMALIES</h4>
            <div style="display:flex; justify-content: space-between; font-size: 0.85em; color: #53FF45;">
                <div>IF:<br><span style="color:#fff; font-size:1.2em;">{tot_if}</span></div>
                <div>SVM:<br><span style="color:#fff; font-size:1.2em;">{tot_svm}</span></div>
                <div>LOF:<br><span style="color:#fff; font-size:1.2em;">{tot_lof}</span></div>
            </div>
        </div>
        """

    left_sidebar = f"""
    <div class="warning-tape"> ML // INFERENCE </div>
    <div style="padding: 15px;">
        {stats_html}
        <div class="warning-tape" style="margin-top: 20px;">MAP // TOGGLES</div>
        <div id="layer-target" style="margin-top: 10px; width: 100%;"></div>
    </div>
    """

    dist_divs, dist_scripts = "", ""
    for param in gdf['sensor_parameter'].unique():
        vals = gdf[gdf['sensor_parameter'] == param]['ground_value'].dropna().tolist()
        color = POLLUTANT_COLORS.get(param.lower(), "#fff")
        div_id = f"dist-{param}"
        dist_divs += f"<div id='{div_id}' style='width:100%; height:180px; margin-bottom:15px; border: 1px solid #333;'></div>"
        dist_scripts += f"""
        Plotly.newPlot('{div_id}', 
            [{{x: {vals}, type: 'histogram', marker: {{color: '{color}', line: {{color: '#050505', width: 2}}}}}}], 
            {{paper_bgcolor: '#050505', plot_bgcolor: '#050505', margin: {{l: 30, r: 10, t: 30, b: 20}}, title: {{text: 'FREQ: {param.upper()}', font: {{color: '{color}', size: 11, family: 'Roboto'}}}}, xaxis: {{gridcolor: '#1a1a1a', tickfont: {{size:9, color:'#53FF45'}}}}, yaxis: {{gridcolor: '#1a1a1a', tickfont: {{size:9, color:'#53FF45'}}}}}}, 
            {{displayModeBar: false}}
        );
        """

    right_sidebar = f"""
    <div class="warning-tape">DATA // DISTRIBUTIONS</div>
    <div style="padding: 15px;">
        {dist_divs}
    </div>
    <script>{dist_scripts}</script>
    """

    custom_css_js = """
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body { display: flex; flex-direction: row; margin: 0; padding: 0; background-color: #050505; color: #53FF45; font-family: 'Roboto', monospace; height: 100vh; overflow: hidden; }
        .sidebar-left { width: 22%; background-color: #050505; border-right: 2px solid #FF4500; overflow-y: auto; z-index: 9999; display: flex; flex-direction: column; }
        .sidebar-right { width: 22%; background-color: #050505; border-left: 2px solid #FF4500; overflow-y: auto; z-index: 9999; display: flex; flex-direction: column; }
        .map-wrapper { flex: 1; height: 100% !important; position: relative !important; }
        
        .warning-tape {
            color: #fff; text-shadow: 1px 1px 2px #000; font-weight: bold; padding: 10px; text-align: center; letter-spacing: 2px; border-bottom: 2px solid #FF4500; border-top: 2px solid #FF4500;
        }

        #layer-target .leaflet-control-layers {
            background: rgba(0,0,0,0.8) !important; border: 1px solid #53FF45 !important; border-radius: 0 !important;
            color: #53FF45 !important; box-shadow: none !important; width: 100%; margin: 0; padding: 10px; box-sizing: border-box;
        }
        .leaflet-control-layers-list { font-family: 'Roboto', monospace; font-size: 0.9em; text-transform: uppercase; }
        .leaflet-control-layers-separator { border-top: 1px solid #FF4500 !important; margin: 10px 0 !important; }
        input[type="checkbox"] { accent-color: #FF4500; }

        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #050505; }
        ::-webkit-scrollbar-thumb { background: #FF4500; }
        ::-webkit-scrollbar-thumb:hover { background: #53FF45; }
    </style>
    <script>
        window.addEventListener('DOMContentLoaded', (event) => {
            setTimeout(() => {
                const layerControl = document.querySelector('.leaflet-control-layers');
                const target = document.getElementById('layer-target');
                if(layerControl && target) { target.appendChild(layerControl); }
            }, 500);
        });
    </script>
    """

    map_html = map_html.replace('<head>', f'<head>\n{custom_css_js}')
    map_html = map_html.replace('<body>', f'<body>\n<div class="sidebar-left">{left_sidebar}</div>\n<div class="map-wrapper">')
    map_html = map_html.replace('</body>', f'</div>\n<div class="sidebar-right">{right_sidebar}</div>\n</body>')

    local_dir = "/app/data"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "montreal_anomalies_v5.html")
    with open(local_path, "w", encoding="utf-8") as f: f.write(map_html)
    print(f"Dashboard saved locally to {local_path}")

    if storage_client:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob("maps/montreal_anomalies_latest.html")
        blob.upload_from_string(map_html, content_type="text/html")
        blob.make_public()
        print(f"Shareable Link: {blob.public_url}")

if __name__ == "__main__":
    create_anomaly_map()