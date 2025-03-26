#!/usr/bin/env python
# coding: utf-8

# In[3]:


# Import necessary libraries

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import requests



# In[4]:


spark = SparkSession.builder \
    .appName("Write to PostgreSQL") \
    .config("spark.jars", "/Users/rigvedavangipurapu/Documents/AirQualityProject/Spark_Utils/postgresql-42.7.4.jar") \
    .getOrCreate()


# In[ ]:


from datetime import datetime, timedelta


def get_dates():
    today = datetime.today()

    # Get yesterday's date
    yesterday = today - timedelta(days=1)

    # Get the day before yesterday's date
    day_before_yesterday = today - timedelta(days=2)

    # Format the dates as 'YYYY-MM-DD'
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    day_before_yesterday_str = day_before_yesterday.strftime('%Y-%m-%d')

    return yesterday_str, day_before_yesterday_str




# In[5]:


headers = {
    "X-API-Key": "9b7c23f6701f7f8e923a5691c6b67d1361bd044b308a8f863502d1190cbe7435"
    
}


# In[6]:


def get_sensor_data_for_city(city_data: tuple) -> List[Tuple[str, Dict]]:
    """
    Helper function to process a single city. This runs on executor nodes.
    Returns list of (city, sensor_dict) tuples.
    """
    city, (lat, lon) = city_data
    base_url = "https://api.openaq.org/v3/locations"
    params = {
        'coordinates': f"{lat},{lon}",
        'radius': 15000,
        'limit': 15
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        sensors = []
        for location in data.get('results', []):
            for sensor in location.get('sensors', []):
                sensors.append({
                    'sensor_id': sensor['id'],
                    'parameter': sensor['parameter']['name'],
                    'units': sensor['parameter']['units'],
                    'location_name': location['name']
                })
        # Return list of (city, sensor) tuples
        return [(city, sensor) for sensor in sensors]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return []
    
    


# In[7]:


def get_sensor_ids(coordinates_dict: Dict):

    # Convert coordinates dictionary to list of tuples for parallelization
    cities_data = list(coordinates_dict.items())
    
    # Create RDD from cities data and collect sensor information
    cities_rdd = spark.sparkContext.parallelize(cities_data)
    sensor_data_rdd = cities_rdd.flatMap(get_sensor_data_for_city)
    
    return sensor_data_rdd


# In[8]:


def fetch_measurements_for_sensors(sensor_ids_rdd: 'RDD[str]', date_to: str, date_from: str) -> 'RDD[Tuple[str, Dict]]':
    """
    Returns RDD of (sensor_id, measurement_dict) tuples.
    """
    def fetch_sensor_measurements(sensor_id: str) -> List[Tuple[str, Dict]]:
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/daily"
        params = {'datetime_to': date_to, 'datetime_from': date_from}
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            measurements = response.json()
            return [(sensor_id, measurement) for measurement in measurements.get('results', [])]
        except Exception as e:
            print(f"Error fetching {sensor_id}: {e}")
            return []
    
    return sensor_ids_rdd.flatMap(fetch_sensor_measurements)


# In[9]:


def analyze_city_data(cities, date_to, date_from):
    """
    Main function to analyze air quality data for all cities.
    """
    # Get sensor IDs for all cities
    print("Fetching sensor information...")
    sensor_data_rdd = get_sensor_ids(cities)  # RDD[(city, sensor_dict)]
# Display the dataset
    # print('Sensor Data RDD :',sensor_data_rdd.show(5))

    # Create (sensor_id, city) pairs
    sensor_city_rdd = sensor_data_rdd.flatMap(
        lambda x: [(x[1]['sensor_id'], x[0])]  # (sensor_id, city)
    )
# Display the dataset
    # print('Sensor City RDD :',sensor_city_rdd.show(5))


    sensor_ids_rdd = sensor_city_rdd.keys()  # RDD[sensor_id]
# Display the dataset
    # print('Sensor IDs RDD :',sensor_ids_rdd.show())


    # Use correct function with date parameters
    measurements_rdd = fetch_measurements_for_sensors(sensor_ids_rdd, date_to, date_from)
# Display the dataset
    # print('Measurements RDD :',measurements_rdd.show())


    joined_rdd = sensor_city_rdd.join(measurements_rdd)  # RDD[(sensor_id, (city, measurement_dict))]
    
    # Convert to Row objects
    def to_row(sensor_data: Tuple[str, Tuple[str, Dict]]) -> Row:
        sensor_id, (city, measurement) = sensor_data
        return Row(
            city=city,
            location=measurement.get('location', 'Unknown'),
            parameter=measurement.get('parameter', {}).get('name', 'Unknown'),
            units=measurement.get('parameter', {}).get('units', 'Unknown'),
            date=measurement.get('period', {}).get('datetimeTo', {}).get('utc', ''),
            value=measurement.get('value'),
            sensor_id=sensor_id
        )
    
# Apply transformation to the dataset
    final_rdd = joined_rdd.map(to_row)
    
    return spark.createDataFrame(final_rdd)


# In[10]:


la_df = analyze_city_data(
    {"Los Angeles": (34.0522, -118.2437)},
    date_to='2020-12-31',
    date_from='2020-01-01'
)
# Display the dataset
la_df.show()
la_df.select('sensor_id').distinct().collect()


# In[23]:


au_df = analyze_city_data(    
     {"Phoenix": (33.4484, -112.0740)},
    date_to='2020-12-31',
    date_from='2020-01-01'
)

au_df.show()



# In[11]:


la_df.count()


# In[12]:


from pyspark.sql.functions import col, count, date_format

# Convert date column to the correct format and extract year-month
la_df = la_df.withColumn("month", date_format(col("date"), "yyyy-MM"))

# Group by month and count occurrences
monthly_counts = la_df.groupBy("month").count().orderBy("month")

# Show results
monthly_counts.show()


# In[13]:


la_df.select('sensor_id','parameter').distinct().collect()



# In[14]:


ny_df = analyze_city_data(
    {"New York": (40.7128, -74.0060)},
    date_to='2020-12-31',
    
    date_from='2020-01-01'
)
# Display the dataset
ny_df.show()


# In[ ]:


ny_df.select('sensor_id').distinct().collect()
ny_df.select('sensor_id','parameter').distinct().collect()





# In[ ]:


from pyspark.sql.functions import col, count, date_format

# Convert date column to the correct format and extract year-month
ny_df = ny_df.withColumn("month", date_format(col("date"), "yyyy-MM"))

# Group by month and count occurrences
monthly_counts = ny_df.groupBy("month").count().orderBy("month")

# Show results
monthly_counts.show()

ny_df.count()



# In[ ]:


ny_df.select('location').distinct().collect()


# In[ ]:


properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


# In[ ]:


url = "jdbc:postgresql://localhost:5432/openaq"

ny_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/openaq") \
    .option("dbtable", "dummy_city") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()




# In[ ]:


#how to insert data into table in a for loop?
#create a function to write the data to the DB

def write_to_db(df):
    properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
    }

    url = "jdbc:postgresql://localhost:5432/openaq"

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/openaq") \
        .option("dbtable", "dummy_city") \
        .option("user", "postgres") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



# In[27]:


import time
coordinates_dict = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "San Francisco": (37.7749, -122.4194),
    "Chicago": (41.8781, -87.6298),
    "Miami": (25.7617, -80.1918),
    "Austin": (30.2672, -97.7431),
    "Phoenix": (33.4484, -112.0740),
    "Seattle": (47.6062, -122.3321),
    "Philadelphia": (39.9526, -75.1652),
    "Boston": (42.3601, -71.0589),
    }


date_to, date_from = get_dates()
for key,value in coordinates_dict.items():
    time.sleep(120)
    df  = analyze_city_data({key:value},date_to=date_to,date_from=date_from)
    print('Got items from API',df.count(),'for ',key)
    write_to_db(df)
    print('writing to db', key)
    time.sleep(120)
    print('waiting....')


# In[ ]:


spark.stop()


# In[ ]:





# psql -U postgres -d openaq
