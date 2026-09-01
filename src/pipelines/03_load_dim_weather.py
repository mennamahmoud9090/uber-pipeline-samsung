from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date

def load_dim_weather():
    spark = SparkSession.builder \
        .appName("Load_Dim_Weather") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    silver_weather_path = "hdfs://localhost:9000/data/silver/weather.csv"
    df_weather = spark.read.option("header", "true").csv(silver_weather_path)

    df_dim_weather = df_weather.select(
        date_format(to_date(col("date")), "yyyyMMdd").cast("int").alias("weather_id"),
        col("temp_avg").cast("double"),
        col("precipitation").cast("double"),
        col("snow").cast("double"),
        col("weather_condition")
    )

    df_dim_weather.writeTo("taxi_catalog.nyc_taxi_dwh.dim_weather").append()
    print("تم تحميل dim_weather بنجاح!")

if __name__ == "__main__":
    load_dim_weather()
