

WITH satellite_stats AS (
    SELECT
        timestamp,
        parameter,
        measurement_value,
        geom,
        AVG(measurement_value) OVER (PARTITION BY parameter) AS mean_val,
        NULLIF(STDDEV(measurement_value) OVER (PARTITION BY parameter), 0) AS stddev_val
    FROM "montreal_methane"."public"."satellite_measurements"
),

ground_stats AS (
    SELECT
        timestamp,
        parameter,
        measurement_value,
        unit,
        geom,
        AVG(measurement_value) OVER (PARTITION BY parameter) AS mean_val,
        NULLIF(STDDEV(measurement_value) OVER (PARTITION BY parameter), 0) AS stddev_val
    FROM "montreal_methane"."public"."openaq_data"
)

SELECT
    sat.timestamp AS satellite_time,
    sen.timestamp AS sensor_time,
    sat.parameter AS sensor_parameter,
    
    -- Raw values retained for reference and dashboard display
    sat.measurement_value AS satellite_value,
    sen.measurement_value AS ground_value,
    sen.unit AS sensor_unit,
    
    -- Z-Scores calculated
    COALESCE((sat.measurement_value - sat.mean_val) / sat.stddev_val, 0) AS sat_z_score,
    COALESCE((sen.measurement_value - sen.mean_val) / sen.stddev_val, 0) AS sen_z_score,
    
    -- Normalized Variance (difference between Z-scores)
    ABS(
        COALESCE((sat.measurement_value - sat.mean_val) / sat.stddev_val, 0) -
        COALESCE((sen.measurement_value - sen.mean_val) / sen.stddev_val, 0)
    ) AS value_variance,
    
    sen.geom AS sensor_location,
    sat.geom AS satellite_footprint

FROM satellite_stats sat
JOIN ground_stats sen
  ON ST_Contains(sat.geom, sen.geom)
  AND sat.parameter = sen.parameter
WHERE sat.timestamp::date = sen.timestamp::date