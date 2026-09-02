from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, hour, minute, second

def load_dim_date():
    spark = SparkSession.builder \
        .appName("Load_Dim_Date") \
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

    df_dim_date = df_trips.select(
        to_date(col("pickup_datetime")).alias("full_date"),
        col("pickup_datetime_year").alias("year"),
        col("pickup_datetime_month").alias("month"),
        col("pickup_datetime_day").alias("day"),
        col("pickup_datetime_hour").alias("hour"),
        col("pickup_datetime_minute").alias("minute"),
        col("pickup_datetime_second").alias("second")
    ).distinct()

    df_dim_date.writeTo("taxi_catalog.nyc_taxi_dwh.dim_date").append()
    print("تم تحميل dim_date بنجاح!")

if __name__ == "__main__":
    load_dim_date()
