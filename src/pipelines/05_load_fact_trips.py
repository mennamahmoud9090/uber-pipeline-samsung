from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_fact_trips():
    spark = SparkSession.builder \
        .appName("Load_Fact_Trips") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    silver_trips_path = "hdfs:///user/student/cleanedUber/trips"
    df_trips = spark.read.parquet(silver_trips_path)

    df_fact_trips = df_trips.select(
        col("PULocationID").cast("int").alias("pu_location_id"),
        col("DOLocationID").cast("int").alias("do_location_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("request_datetime"),
        col("on_scene_datetime"),
        col("trip_miles").cast("double"),
        col("trip_time").cast("long"),
        col("base_passenger_fare").cast("double"),
        col("tolls").cast("double"),
        col("bcf").cast("double"),
        col("sales_tax").cast("double"),
        col("congestion_surcharge").cast("double"),
        col("airport_fee").cast("double"),
        col("tips").cast("double"),
        col("driver_pay").cast("double"),
        col("cbd_congestion_fee").cast("double")
    )

    df_fact_trips.writeTo("taxi_catalog.nyc_taxi_dwh.fact_trips").append()
    print("تم تحميل fact_trips بنجاح!")

if __name__ == "__main__":
    load_fact_trips()
