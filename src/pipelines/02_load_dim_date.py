from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofweek, month, year, quarter, expr, to_date

def load_dim_date():
    spark = SparkSession.builder \
        .appName("Load_Dim_Date") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    silver_trips_path = "hdfs://localhost:9000/data/silver/tripDataCleaned.parquet"
    df_trips = spark.read.parquet(silver_trips_path)

    df_dates = df_trips.select(to_date(col("pickup_datetime")).alias("full_date")).distinct()

    df_dim_date = df_dates.select(
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_id"),
        col("full_date"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        dayofweek(col("full_date")).alias("day_of_week"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        quarter(col("full_date")).alias("quarter"),
        year(col("full_date")).alias("year"),
        expr("dayofweek(full_date) IN (1, 7)").alias("is_weekend")
    )

    df_dim_date.writeTo("taxi_catalog.nyc_taxi_dwh.dim_date").append()
    print("تم تحميل dim_date بنجاح!")

if __name__ == "__main__":
    load_dim_date()
