create table f_city_sensor_pollutant_data
(city varchar(100), 
location varchar(100)
,pollutant_name varchar(50),
units varchar(20) ,
date varchar(50),
pollutant_value numeric,
sensor_id TEXT,
aqi_value NUMERIC,
hash_key TEXT
);

insert into f_city_sensor_pollutant_data (city,location, pollutant_name, units, date, pollutant_value, sensor_id, aqi_value, hash_key)
select 
    city,
    location,
    parameter as pollutant_name,
    units,
    date,
    value as pollutant_value,
    sensor_id,
    aqi_value,
    hash_key
from city_sensor_data1 ;