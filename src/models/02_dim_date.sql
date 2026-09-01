CREATE TABLE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh.dim_date (
    date_id INT,
    full_date DATE,
    day_name STRING,
    day_of_week INT,
    month INT,
    month_name STRING,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
)
USING iceberg
TBLPROPERTIES ('write.format.default'='parquet');
