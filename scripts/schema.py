import os
from sqlalchemy import Column, Integer, Float, DateTime, String, create_engine
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class SatelliteMeasurement(Base):
    __tablename__ = 'satellite_measurements'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    parameter = Column(String, nullable=False)
    measurement_value = Column(Float)
    # The spatial_index=True creates an R-Tree index in PostGIS for fast spatial queries
    geom = Column(Geometry(geometry_type='POLYGON', srid=4326, spatial_index=True))

class TerrestrialSensor(Base):
    __tablename__ = 'openaq_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor_name = Column(String)
    timestamp = Column(DateTime, nullable=False)
    parameter = Column(String, nullable=False)
    measurement_value = Column(Float)
    unit = Column(String)
    geom = Column(Geometry(geometry_type='POINT', srid=4326, spatial_index=True))

def init_db():
    db_user = os.getenv("DB_USER", "gis_user")
    db_pass = os.getenv("DB_PASS", "gis_pass")
    db_host = os.getenv("DB_HOST", "postgis")
    db_name = os.getenv("DB_NAME", "montreal_methane")
    
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    engine = create_engine(db_url)
    
    Base.metadata.create_all(engine)
    print("Spatial tables verified/created successfully in PostGIS.")

if __name__ == "__main__":
    init_db()