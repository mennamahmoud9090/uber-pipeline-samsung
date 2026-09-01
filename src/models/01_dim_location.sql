CREATE DATABASE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh;

CREATE TABLE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh.dim_location (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
USING iceberg
TBLPROPERTIES ('write.format.default'='parquet');
