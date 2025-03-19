create table d_sensor
(
    sensor_id text PRIMARY KEY,
    city varchar(100),
    pollutant_name varchar(50),
    start_date varchar(50),
    end_date varchar(50)    
);

insert into d_sensor (sensor_id, city, pollutant_name, start_date, end_date)
(
    select 
        f.sensor_id,
        f.city,
        f.pollutant_name,
        min(date) as start_date,
        max(date) as end_date
    from f_city_sensor_pollutant_data f
    group by 1,2,3
);


