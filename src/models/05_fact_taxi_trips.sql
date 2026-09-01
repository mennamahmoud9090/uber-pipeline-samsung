CREATE TABLE IF NOT EXISTS taxi_catalog.nyc_taxi_dwh.fact_taxi_trips (
    trip_id BIGINT,
    pickup_date_id INT,
    date_hour_id BIGINT,
    pickup_location_id INT,
    dropoff_location_id INT,
    trip_attribute_id INT,
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_miles DOUBLE,
    trip_time INT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    cbd_congestion_fee DOUBLE
)
USING iceberg
PARTITIONED BY (months(pickup_datetime))
TBLPROPERTIES ('write.format.default'='parquet');
