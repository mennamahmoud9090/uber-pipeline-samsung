from pathlib import Path
from pyspark.sql import SparkSession

def create_dwh_tables():
    spark = SparkSession.builder \
        .appName("Create_Iceberg_DWH_Tables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.taxi_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.taxi_catalog.type", "hadoop") \
        .config("spark.sql.catalog.taxi_catalog.warehouse", "hdfs://localhost:9000/user/hive/warehouse/taxi_catalog") \
        .getOrCreate()

    models_dir = Path("src/models")
    sql_files = sorted(models_dir.glob("*.sql"))

    if not sql_files:
        print("لم يتم العثور على ملفات .sql داخل src/models/")
        return

    for sql_file in sql_files:
        print(f"جاري تنفيذ: {sql_file.name} ...")
        with open(sql_file, "r") as f:
            sql_content = f.read().strip()
            if sql_content:
                spark.sql(sql_content)
                print(f"تم إنشاء الجدول من {sql_file.name} بنجاح!")

    print("\nتمت عملية إنشاء جميع جداول DWH Iceberg بنجاح.")

if __name__ == "__main__":
    create_dwh_tables()
