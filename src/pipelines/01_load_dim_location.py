from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_dim_location():
    spark = SparkSession.builder \
        .appName("Load_Dim_Location") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    silver_zone_path = "hdfs://localhost:9000/data/silver/Taxi_LookupCleaned"
    df_zones = spark.read.option("header", "true").csv(silver_zone_path)

    df_dim_location = df_zones.select(
        col("LocationID").cast("int").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone").alias("service_zone")
    )

    df_dim_location.writeTo("taxi_catalog.nyc_taxi_dwh.dim_location").append()
    print("تم تحميل dim_location بنجاح!")

if __name__ == "__main__":
    load_dim_location()
