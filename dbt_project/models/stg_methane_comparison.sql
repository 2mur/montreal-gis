{{ config(materialized='table') }}

SELECT
    sat.timestamp AS satellite_time,
    sen.timestamp AS sensor_time,
    sat.ch4_column_volume AS satellite_ch4,
    sen.parameter AS sensor_parameter,
    sen.measurement_value AS sensor_ch4,
    sen.unit AS sensor_unit,
    ABS(sat.ch4_column_volume - sen.measurement_value) AS ch4_variance,
    sen.geom AS sensor_location,
    sat.geom AS satellite_footprint
FROM {{ source('raw_data', 'satellite_methane') }} sat
JOIN {{ source('raw_data', 'openaq_data') }} sen
  ON ST_Contains(sat.geom, sen.geom)
WHERE sat.timestamp::date = sen.timestamp::date