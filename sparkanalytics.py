#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Setting up Pyspark


# In[1]:


get_ipython().system('pip install -q findspark')
get_ipython().system('pip install pyspark')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types


# In[19]:


spark = SparkSession.builder \
    .master('local') \
    .appName('SparkAnalytics') \
    .getOrCreate()


# In[ ]:


# Download Dataset


# In[20]:


get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-02.parquet')


# In[16]:


# Defining Table Schema
## Spark includes the ability to read and write from a large number of data sources using InferSchema, this will automatically guess the data types for each field.
## Howerever, you should use StructType to define the schema while reading a file for more efficient way to improve the Spark performance.


# In[3]:


schema = types.StructType(
    [
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.DoubleType(), True),
        types.StructField('DOLocationID', types.DoubleType(), True),
        types.StructField('SR_Flag', types.IntegerType(), True),
        types.StructField('Affiliated_base_number', types.StringType(), True)
    ]
)


# In[4]:


df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .parquet('fhv_tripdata_2021-02.parquet')


# In[5]:


df.show()


# In[6]:


# Data Analysis using Spark SQL


# In[9]:


## createOrReplaceTempView() is used when you wanted to store the table for a specific spark session. Once created you can use it to run SQL queries.
df.createOrReplaceTempView("fhv_trip")


# In[10]:


### How many taxi trips were there on February 15?


# In[11]:


taxi_trips_15_feb = spark.sql("""
with trips_15_feb as 
(SELECT
    *
FROM 
    fhv_trip
WHERE
    to_date(pickup_datetime) = '2021-02-15'
)
SELECT
    COUNT(1) as taxi_trips_15_feb
FROM 
    trips_15_feb
WHERE
    to_date(pickup_datetime) = '2021-02-15'
""").show()


# In[15]:


### Find the longest trip for each day!


# In[27]:


taxi_longest_trips = spark.sql("""
                              SELECT
                                  pickup_datetime, dropoff_datetime,
                                  ROUND(((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))/3600),2) AS duration_in_hours
                              FROM fhv_trip
                              SORT BY
                                  duration_in_hours DESC
                              """)

taxi_longest_trips.show()


# In[28]:


### Find Top 5 Most frequent `dispatching_base_num` ?


# In[30]:


most_dispatching_base_num = spark.sql("""
    SELECT 
          dispatching_base_num,
          COUNT(1) as amount
    FROM 
          fhv_trip
    GROUP BY
          1
    ORDER BY
          2 DESC
    LIMIT 
          5
""")

most_dispatching_base_num.show()


# In[31]:


### Find Top 5 Most common location pairs (PUlocationID and DOlocationID)
## Location name is in another file 'taxi_zone_lookup', we need to import in and join into main table


# In[32]:


### Download Dataset

get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')


# In[35]:


### Defining Table Schema
taxi_zones_schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])


# In[36]:


taxi_zones_df = spark.read.option('header', 'true').schema(taxi_zones_schema).csv('taxi+_zone_lookup.csv')


# In[39]:


taxi_zones_df.show()


# In[43]:


### Schema for Pick Up Zone
Pickup_table = taxi_zones_df \
    .withColumnRenamed('Zone', 'PickUp_Zone') \
    .withColumnRenamed('LocationID', 'PickUp_Location_ID') \
    .withColumnRenamed('Borough', 'PickUP_Borough') \
    .drop('service_zone')

### Schema for Drop Off Zone
Dropoff_table = taxi_zones_df \
    .withColumnRenamed('Zone', 'DropOff_Zone') \
    .withColumnRenamed('LocationID', 'DropOff_Location_ID') \
    .withColumnRenamed('Borough', 'DropOff_Borough') \
    .drop('service_zone')


# In[51]:


# Join fhv_trip table with the table taxi_zone table
join_test = df.join(Pickup_table, df.PULocationID == Pickup_table.PickUp_Location_ID)
df_join = join_test.join(Dropoff_table, join_test.DOLocationID == Dropoff_table.DropOff_Location_ID)


# In[52]:


df_join.show(3)


# In[53]:


df_join.createOrReplaceTempView("location_pairs")


# In[55]:


location_pairs = spark.sql("""
SELECT
    CONCAT(coalesce(PickUp_Zone, 'Unknown'), '/', coalesce(DropOff_Zone, 'Unknown')) AS zone_pair,
    COUNT(1) as total_count
FROM
    location_pairs
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT
    5
;
""")

location_pairs.show()


# In[ ]:




