#!/usr/bin/env python
# coding: utf-8

# In[ ]:


''' sql statements

create table city_sensor_data1 
(city varchar(100), location varchar(100) 
,parameter varchar(50), 
units varchar(20) , 
date varchar(50), 
value numeric, 
sensor_id integer);

INSERT INTO city_sensor_data1 (city, location, parameter, units, date, value, sensor_id)  
SELECT DISTINCT city, location, parameter, units, date, value, sensor_id  
FROM dummy_city;

ALTER TABLE city_sensor_data1  
ADD COLUMN aqi_value NUMERIC;

ALTER TABLE city_sensor_data1  
ADD COLUMN hash_key TEXT;

UPDATE city_sensor_data1  
SET hash_key = md5(city || date || sensor_id);

ALTER TABLE city_sensor_data1  
ADD CONSTRAINT city_sensor_pk PRIMARY KEY (hash_key);

ALTER TABLE city_sensor_data1  
ADD COLUMN processed BOOLEAN DEFAULT FALSE;
'''


# In[9]:


pip install python-aqi


# In[7]:


get_ipython().system(' yes y | pip uninstall psycopg2')
get_ipython().system(' yes y | pip uninstall psycopg2-binary')
get_ipython().system(' pip install psycopg2-binary --no-cache-dir')


# In[10]:


import aqi
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import requests
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType
import aqi



# In[11]:


# Initialize Spark session
spark = SparkSession.builder \
    .appName("AQI Reader") \
    .config("spark.jars", "/Users/rigvedavangipurapu/Documents/AirQualityProject/Spark_Utils/postgresql-42.7.4.jar") \
    .getOrCreate()


# In[12]:


from pyspark.sql import SparkSession

def read_from_db():

    # JDBC connection properties
    properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    
    url = "jdbc:postgresql://localhost:5432/openaq"

    # Read data into DataFrame
    df = spark.read.jdbc(url=url, table="city_sensor_data1", properties=properties)
    
    return df

# Example usage
df = read_from_db()
df.show()
type(df)



# In[13]:


# Define AQI Calculation Function
def calculate_aqi(parameter, value):
    """
    Compute AQI using python-aqi library. If the parameter is unsupported, return None.
    """
    try:
        pollutant_map = {
            "pm25": aqi.POLLUTANT_PM25,
            "pm10": aqi.POLLUTANT_PM10,
            "o3": aqi.POLLUTANT_O3_8H,
            "co": aqi.POLLUTANT_CO_8H,
            "so2": aqi.POLLUTANT_SO2_1H,
            "no2": aqi.POLLUTANT_NO2_1H
        }

        # print("pollutant received",parameter)
        # print(parameter.lower() in pollutant_map)

        if parameter.lower() in pollutant_map:
        	return int(aqi.to_iaqi(pollutant_map[parameter.lower()], value)) #The AQI calculated for a single pollutant 
        else:
            return None  # Skip unsupported parameters like 'bc'

    except Exception:
        return None  # Handle errors gracefully


# In[14]:


aqi_udf = udf(calculate_aqi, IntegerType())


# In[18]:


# Set batch size
batch_size = 1000  # Number of rows processed per batch


# In[19]:


import psycopg2

def update_processed_rows(ids):
    if not ids:
        return  # No updates needed

    try:
        # Using context manager to ensure proper closing of connection and cursor
        with psycopg2.connect(
            dbname="openaq",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        ) as conn:
            with conn.cursor() as cur:
                # Create the SQL query to update the processed flag
                id_list = ",".join(f"'{id}'" for id in ids)  # Ensure proper SQL formatting
                update_query = f"""
                    UPDATE city_sensor_data1
                    SET processed = TRUE
                    WHERE hash_key IN ({id_list});
                """
                
                # Log the query for debugging purposes
                print(f"Executing query: {update_query}")
                
                # Execute the query
                cur.execute(update_query)
                conn.commit()

                print(f"Updated {len(ids)} rows as processed.")

    except Exception as e:
        print(f"Error updating processed rows: {e}")


# In[20]:


properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
jdbc_url = "jdbc:postgresql://localhost:5432/openaq"
while True:
    # Read the next batch of unprocessed rows
    df_batch = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT hash_key, parameter, value FROM city_sensor_data1 WHERE processed = FALSE LIMIT {batch_size}) AS batch",
        properties=properties
    )

    if df_batch.count() == 0:
        print("No more unprocessed rows found. Stopping.")
        break  # Stop if all rows are processed

    # Compute AQI
    df_batch = df_batch.withColumn("aqi_value", aqi_udf(col("parameter"), col("value")))

    # Filter out rows where AQI is None (i.e., unsupported pollutants)
    df_batch = df_batch.filter(col("aqi_value").isNotNull())

    # df_batch.show()
    
    # Write the updated batch to a temporary table
    df_batch.select("hash_key", "aqi_value").write \
        .jdbc(url=jdbc_url, table="city_sensor_updates", mode="append", properties=properties)
    

    # Update the processed rows in the main table
    ids = [str(row.hash_key) for row in df_batch.select("hash_key").collect()]
    print(len(ids))
    if ids:
        id_list = ",".join(ids)
        update_processed_rows(ids) 

    print(f"Processed {len(ids)} rows")

