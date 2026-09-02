from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_dim_trip_attributes():
    spark = SparkSession.builder \
        .appName("Load_Dim_Trip_Attributes") \
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

    df_dim_trip_attributes = df_trips.select(
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),
        col("shared_request_flag"),
        col("shared_match_flag"),
        col("access_a_ride_flag"),
        col("wav_request_flag"),
        col("wav_match_flag"),
        col("is_Paid")
    ).distinct()

    df_dim_trip_attributes.writeTo("taxi_catalog.nyc_taxi_dwh.dim_trip_attributes").append()
    print("تم تحميل dim_trip_attributes بنجاح!")

if __name__ == "__main__":
    load_dim_trip_attributes()
