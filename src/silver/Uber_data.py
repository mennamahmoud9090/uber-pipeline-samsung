# Databricks notebook source
#Libraries needed
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("SHOW VOLUMES in workspace.sic").show()

# COMMAND ----------

#Reading Data
taxi_zone = spark.read.csv('/Volumes/workspace/sic/taxilookup',
inferSchema = True,
header= True)
tripdata_2025 = spark.read.parquet("/Volumes/workspace/sic/tripdata_2025")
tripdata_2026 = spark.read.parquet("/Volumes/workspace/sic/tripdata_2026")

# COMMAND ----------

taxi_zone.show(truncate=False)

# COMMAND ----------

tripdata_2025.show(truncate=False)

# COMMAND ----------

tripdata_2026.show(truncate=False)

# COMMAND ----------

tripdata_2026.printSchema()

# COMMAND ----------

tripdata_2025.printSchema()

# COMMAND ----------

taxi_zone.printSchema()

# COMMAND ----------

#Check Null values for each column
tripdata_2026.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in tripdata_2026.columns
]).show()


# COMMAND ----------

tripdata_2025.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in tripdata_2025.columns
]).show()

# COMMAND ----------

taxi_zone.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in taxi_zone.columns
]).show()


# COMMAND ----------

null_counts = tripdata_2025.select(
    count(when(col("originating_base_num").isNull(), 1)).alias("null_count"),
    count(when(col("originating_base_num").isNotNull(), 1)).alias("not_null_count")
)
pdf = null_counts.toPandas()

# COMMAND ----------

!pip install matplotlib

# COMMAND ----------

#Simple visualization to show how many nulls in one column
import matplotlib.pyplot as plt

values = [
    pdf["null_count"][0],
    pdf["not_null_count"][0]
]

labels = ["NULL", "Not NULL"]


plt.bar(labels, values)
plt.title("Missing Values in originating_base_num")
plt.xlabel("Value status")
plt.ylabel("Number of records")

plt.show()

# COMMAND ----------

taxi_zoneCleaned=taxi_zone.dropDuplicates(["LocationID"])

# COMMAND ----------

tripdata_2025Cleaned= tripdata_2025.fillna({
 'originating_base_num': "Unknown"
})
tripdata_2026Cleaned=tripdata_2026.fillna({
 'originating_base_num': "Unknown"
})

# COMMAND ----------

#Checking any duplicates
original_count = tripdata_2025.count()
cleaned_count = tripdata_2025Cleaned.count()

print("Original:", original_count)
print("Cleaned:", cleaned_count)
print("Removed:", original_count - cleaned_count)

# COMMAND ----------

#Merge
tripdata_cleaned = tripdata_2025Cleaned.unionByName(
    tripdata_2026Cleaned
)

# COMMAND ----------

tripdata_cleaned.show()

# COMMAND ----------

tripdata_Cleaned=tripdata_cleaned.dropDuplicates()

# COMMAND ----------

tripdata_Cleaned.count()

# COMMAND ----------

tripdata_Cleaned.filter(
    col("base_passenger_fare") < 0
).count()

# COMMAND ----------

tripdata_Cleaned=tripdata_Cleaned.withColumn('is_Paid',
                                                when(col("base_passenger_fare") < 0, "Refunded")
                                                .otherwise("Paid"))
tripdata_Cleaned.show()


# COMMAND ----------

tripdata_Cleaned.coalesce(1).write.mode("overwrite").parquet(
    "/Volumes/workspace/sic/cleanedtrip_data"
)

# COMMAND ----------

taxi_zoneCleaned.write.mode("overwrite").option("header", "true").csv(
    "/Volumes/workspace/sic/cleaned_taxizone"
)
