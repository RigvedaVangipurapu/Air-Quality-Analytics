--Average Pollutant Levels per Pollutant Type per month, city 

SELECT  
    city,
    EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD')) AS month_key,
    EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year_key,
    pollutant_name,
    ROUND(avg(pollutant_value),2),
    units
FROM f_city_sensor_pollutant_data
GROUP BY CITY, month_key, year_key, pollutant_name, units
ORDER BY CITY, month_key, year_key, pollutant_name;

-- Most Monitored Pollutant per City per month
WITH POLLUTANT_COUNT_CALC AS
(
 SELECT 
        pollutant_name,
        city,
        EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD')) AS month_key,
        EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year_key,
        COUNT(*) AS pollutant_count
    FROM f_city_sensor_pollutant_data
    GROUP BY city, pollutant_name, month_key, year_key
)
, RANKED AS 
(
    SELECT  
        pollutant_name,
        city,
        pollutant_count,
        month_key,
        year_key,
        RANK() OVER (PARTITION BY city,month_key,year_key ORDER BY pollutant_count DESC) AS Rank
FROM POLLUTANT_COUNT_CALC
)
SELECT
    city,
    month_key,
    year_key,
    pollutant_name,
    pollutant_count as num_measurements
FROM RANKED
WHERE Rank = 1
order by 
;


--Cities with the Highest and lowest AQI values Monthly
WITH RANKS AS (
    SELECT
            city,
            round(aqi_monthly_avg,1) as aqi_monthly_avg,
            month_key,
            DENSE_RANK() OVER (PARTITION BY month_key ORDER BY aqi_monthly_avg desc) as highest_rank,
            DENSE_RANK() OVER (PARTITION BY month_key ORDER BY aqi_monthly_avg asc) as lowest_rank
    from city_aqi
    where aqi_monthly_avg !=0
)
SELECT 
    month_key,
    max(case when highest_rank = 1 then city end) as highest_aqi_city,
    max(case when highest_rank = 1 then aqi_monthly_avg end) as highest_aqi,
    max(case when highest_rank = 2 then city end) as second_highest_aqi_city,
    max(case when highest_rank = 2 then aqi_monthly_avg end) as second_highest_aqi,
    max(case when lowest_rank = 1 then city end) as lowest_aqi_city,
    max(case when lowest_rank = 1 then aqi_monthly_avg end) as lowest_aqi,
    max(case when lowest_rank = 2 then city end) as second_lowest_aqi_city,
    max(case when lowest_rank = 2 then aqi_monthly_avg end) as second_lowest_aqi
FROM RANKS
WHERE highest_rank = 1 or lowest_rank = 1 or highest_rank = 2 or lowest_rank = 2
group by month_key;