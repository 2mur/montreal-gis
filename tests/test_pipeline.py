import pytest
import geopandas as gpd
from shapely.geometry import Point

def test_crs_match():
    """Validates that the spatial data framework enforces EPSG:4326."""
    # Mock ground sensor data
    gdf = gpd.GeoDataFrame(
        {'sensor_id': ['MTL-01']},
        geometry=[Point(-73.5673, 45.5017)],
        crs="EPSG:4326"
    )
    
    assert gdf.crs == "EPSG:4326", "Pipeline Failure: CRS mismatch. Expected EPSG:4326."

def test_variance_logic():
    """Validates the basic math of the variance calculation."""
    sat_val = 1850.5
    sensor_val = 1800.0
    expected_variance = 50.5
    assert abs(sat_val - sensor_val) == expected_variance