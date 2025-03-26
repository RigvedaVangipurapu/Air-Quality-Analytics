#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import requests



# In[27]:


# Initialize Spark session
spark = SparkSession.builder \
    .appName("AQAnalysis") \
    .getOrCreate()


# In[3]:


headers = {
    "X-API-Key": "9b7c23f6701f7f8e923a5691c6b67d1361bd044b308a8f863502d1190cbe7435"
    
}


# In[4]:


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
    
    


# In[5]:


def get_sensor_ids(coordinates_dict: Dict):

    # Convert coordinates dictionary to list of tuples for parallelization
    cities_data = list(coordinates_dict.items())
    
    # Create RDD from cities data and collect sensor information
    cities_rdd = spark.sparkContext.parallelize(cities_data)
    sensor_data_rdd = cities_rdd.flatMap(get_sensor_data_for_city)
    
    return sensor_data_rdd


# In[6]:


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


# In[7]:


def analyze_city_data(cities, date_to, date_from):
    """
    Main function to analyze air quality data for all cities.
    """
    # Get sensor IDs for all cities
    print("Fetching sensor information...")
    sensor_data_rdd = get_sensor_ids(cities)  # RDD[(city, sensor_dict)]
    # print('Sensor Data RDD :',sensor_data_rdd.show(5))

    # Create (sensor_id, city) pairs
    sensor_city_rdd = sensor_data_rdd.flatMap(
        lambda x: [(x[1]['sensor_id'], x[0])]  # (sensor_id, city)
    )
    # print('Sensor City RDD :',sensor_city_rdd.show(5))


    sensor_ids_rdd = sensor_city_rdd.keys()  # RDD[sensor_id]
    # print('Sensor IDs RDD :',sensor_ids_rdd.show())


    # Use correct function with date parameters
    measurements_rdd = fetch_measurements_for_sensors(sensor_ids_rdd, date_to, date_from)
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
    
    final_rdd = joined_rdd.map(to_row)
    
    return spark.createDataFrame(final_rdd)


# In[8]:


la_df = analyze_city_data(
    {"Los Angeles": (34.0522, -118.2437)},
    date_to='2020-12-31',
    date_from='2020-01-01'
)
la_df.show()
la_df.select('sensor_id').distinct().collect()


# In[10]:


ny_df = analyze_city_data(
    {"New York": (40.7128, -74.0060)},
    date_to='2020-12-31',
    
    date_from='2020-01-01'
)
ny_df.show()


# In[11]:


ny_df.select('sensor_id').distinct().collect()


# In[12]:


ny_df.select('location').distinct().collect()


# In[13]:


#write data in to table


spark1 = SparkSession.builder \
    .appName("Write to PostgreSQL") \
    .config("spark.jars", "./Spark Utils/postgresql-42.7.4.jar") \
    .getOrCreate()


# In[30]:


properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


# In[31]:


url = "jdbc:postgresql://localhost:5432/openaq"

ny_df.write \
    .jdbc(url=url, table="dummy_city", mode="append", properties=properties)



# In[23]:


import logging
logging.getLogger("py4j").setLevel(logging.DEBUG)

