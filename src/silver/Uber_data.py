#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

#Starting Entry Point
spark = SparkSession.builder     .appName("NYC Taxi")     .master("local[*]")     .getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")


# In[3]:


#Libraries needed
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# In[10]:


weather1=spark.read.json("hdfs:///user/student/raw_uber/weather_api")


# In[11]:


weather1.show()


# In[31]:


weather_final = weather1.select(
    "latitude",
    "longitude",
    "timezone",
    explode(
        arrays_zip(
            col("hourly.time"),
            col("hourly.temperature_2m"),
            col("hourly.precipitation"),
            col("hourly.snowfall")
        )
    ).alias("weather")
).select(
    "latitude",
    "longitude",
    "timezone",
    col("weather.0").alias("time"),
    col("weather.1").alias("temperature_2m"),
    col("weather.2").alias("precipitation"),
    col("weather.3").alias("snowfall")
)


# In[32]:


weather_final.show()


# In[33]:


from pyspark.sql.functions import *
weather_final = weather_final.withColumn(
    "time",
    to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm")
)\
    .withColumn(
        "Date",
        to_date(col("time"), "yyyy-MM-dd'T'HH:mm")
    )\
        .withColumn(
    "hour",
    hour("time")
)


# In[34]:


weathercleaned1= weather_final.filter(col('Date') == '2025-12-01')


# In[35]:


weathercleaned2=weather_final.filter(col('Date') == '2026-01-01')


# In[36]:


weather_cleaned = weathercleaned1.unionByName(
    weathercleaned2
)


# In[38]:


weather_cleaned = (
    weather_cleaned
    .withColumn("year", year(col("Date")))
    .withColumn("month", month(col("Date")))
    .withColumn("day", dayofmonth(col("Date")))
)
weather_cleaned.drop('Date')


# In[44]:


weather_Cleaned= weather_cleaned.drop("time").drop('Date')


# In[45]:


weather_Cleaned.show()


# In[18]:


#Reading Data
taxi_zone = spark.read.csv(r'hdfs:///user/student/raw_uber/rawdata2026.csv',
inferSchema = True,
header= True)
tripdata_2025 = spark.read.parquet(r'hdfs:///user/student/raw_uber/fhvhv_tripdata_2025-12.parquet')
tripdata_2026 = spark.read.parquet(r'hdfs:///user/student/raw_uber/fhvhv_tripdata_2026-01.parquet')


# In[5]:


taxi_zone.show(truncate=False)


# In[6]:


tripdata_2025.show(truncate=False)


# In[7]:


tripdata_2026.show(truncate=False)


# In[7]:


tripdata_2026.printSchema()


# In[14]:


tripdata_2025.printSchema()


# In[15]:


taxi_zone.printSchema()


# In[16]:


#Check Null values for each column
tripdata_2026.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in tripdata_2026.columns
]).show()


# In[17]:


tripdata_2025.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in tripdata_2025.columns
]).show()


# In[12]:


taxi_zone.select([
    sum(col(c).isNull().cast('int')).alias(c)
    for c in taxi_zone.columns
]).show()


# In[18]:


null_counts = tripdata_2025.select(
    count(when(col("originating_base_num").isNull(), 1)).alias("null_count"),
    count(when(col("originating_base_num").isNotNull(), 1)).alias("not_null_count")
)
pdf = null_counts.toPandas()


# In[19]:


# get_ipython().system('pip install matplotlib')


# In[20]:


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


# In[20]:


#Drop Duplicates
tripdata_2025Cleaned = tripdata_2025.dropDuplicates([
    "hvfhs_license_num",
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID"
])
tripdata_2026Cleaned=tripdata_2026.dropDuplicates([
  "hvfhs_license_num",
      "request_datetime",
      "pickup_datetime",
      "dropoff_datetime",
      "PULocationID",
      "DOLocationID"  
])


# In[21]:


tripdata_2025Cleaned= tripdata_2025.fillna({
 'originating_base_num': "Unknown"
})
tripdata_2026Cleaned=tripdata_2026.fillna({
 'originating_base_num': "Unknown"
})


# In[23]:


tripdata_2025Cleaned.show()


# In[24]:


#Checking any duplicates
original_count = tripdata_2025.count()
cleaned_count = tripdata_2025Cleaned.count()

print("Original:", original_count)
print("Cleaned:", cleaned_count)
print("Removed:", original_count - cleaned_count)


# In[25]:


tripdata_2025Cleaned.filter(
    col("base_passenger_fare") < 0
).count()


# In[22]:


taxi_zoneCleaned=taxi_zone.dropDuplicates(["LocationID"])


# In[23]:


tripdata_cleaned = tripdata_2025Cleaned.unionByName(
    tripdata_2026Cleaned
)


# In[12]:


tripdata_cleaned.show()


# In[24]:


tripdata_Cleaned=tripdata_cleaned.dropDuplicates()


# In[14]:


tripdata_Cleaned.filter(
    col("base_passenger_fare") < 0
).count()


# In[25]:


tripdata_Cleaned=tripdata_Cleaned.withColumn('is_Paid',
                                                when(col("base_passenger_fare") < 0, "Refunded")
                                                .otherwise("Paid"))


# In[26]:


datetime_cols = [
    "pickup_datetime",
    "dropoff_datetime"
]

for col_name in datetime_cols:
    tripdata_Cleaned = (
        tripdata_Cleaned
        .withColumn(f"{col_name}_year", year(col(col_name)))
        .withColumn(f"{col_name}_month", month(col(col_name)))
        .withColumn(f"{col_name}_day", dayofmonth(col(col_name)))
        .withColumn(f"{col_name}_hour", hour(col(col_name)))
        .withColumn(f"{col_name}_minute", minute(col(col_name)))
        .withColumn(f"{col_name}_second", second(col(col_name)))
    )


# In[27]:


tripdata_Cleaned.printSchema()


# In[ ]:




