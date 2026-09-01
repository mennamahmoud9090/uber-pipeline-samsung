from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

def load_fact_trips():
    spark = SparkSession.builder \
        .appName("Load_Fact_Trips") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    silver_trips_path = "hdfs://localhost:9000/data/silver/tripDataCleaned.parquet"
    df_trips = spark.read.parquet(silver_trips_path)
    df_attributes = spark.table("taxi_catalog.nyc_taxi_dwh.dim_trip_attributes")

    df_trips_with_date = df_trips.withColumn("date_id", date_format(col("pickup_datetime"), "yyyyMMdd").cast("int"))

    df_fact = df_trips_with_date.join(
        df_attributes,
        (df_trips_with_date.originating_base_num == df_attributes.originating_base_num) &
        (df_trips_with_date.is_Paid == df_attributes.is_Paid),
        "left"
    ).select(
        col("date_id"),
        col("PULocationID").cast("int").alias("pickup_location_id"),
        col("DOLocationID").cast("int").alias("dropoff_location_id"),
        col("attribute_id"),
        col("base_passenger_fare").cast("double").alias("base_passenger_fare")
    )

    df_fact.writeTo("taxi_catalog.nyc_taxi_dwh.fact_taxi_trips").append()
    print("تم تحميل fact_taxi_trips بنجاح!")

if __name__ == "__main__":
    load_fact_trips()
