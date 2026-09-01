CREATE TABLE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh.dim_weather (
    date_hour_id BIGINT,
    date DATE,
    hour INT,
    temperature_2m DOUBLE,
    precipitation DOUBLE,
    snowfall DOUBLE,
    timezone STRING
)
USING iceberg
TBLPROPERTIES ('write.format.default'='parquet');
