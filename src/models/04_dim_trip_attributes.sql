CREATE TABLE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh.dim_trip_attributes (
    trip_attribute_id INT,
    payment_name STRING,
    rate_code_name STRING,
    store_and_fwd_flag STRING,
    shared_request_flag STRING,
    shared_match_flag STRING,
    access_a_ride_flag STRING,
    wav_request_flag STRING,
    wav_match_flag STRING
)
USING iceberg
TBLPROPERTIES ('write.format.default'='parquet');
