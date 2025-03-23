
CREATE TABLE city_aqi ( city varchar(100), date varchar(50), month_key integer, parameter_daily_max  varchar(50), aqi_daily_max numeric, aqi_monthly_avg numeric); 

INSERT INTO city_aqi (city, date, month_key, parameter_daily_max, aqi_daily_max, aqi_monthly_avg)
WITH city_month AS (
    SELECT 
        city, 
        generate_series(1, 12) AS month_number
    FROM city_sensor_data1
    GROUP BY city
),
city_daily AS (
    SELECT 
        city, 
        date,
        EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD')) AS month_key,
        parameter,
        aqi_value 
    FROM (
        SELECT 
            city, 
            date, 
            parameter, 
            aqi_value, 
            RANK() OVER (PARTITION BY city, date ORDER BY aqi_value DESC) AS rnk
        FROM city_sensor_data1
        WHERE aqi_value IS NOT NULL
    ) ranked
    WHERE rnk = 1
),
monthly_avg AS (
    SELECT
        city,
        month_key,
        SUM(aqi_value) / COUNT(*) AS aqi_monthly_avg
    FROM city_daily
    WHERE aqi_value IS NOT NULL AND aqi_value > 0
    GROUP BY city, month_key
)
SELECT
    cm.city,
    cd.date,
    cm.month_number AS month_key,
    COALESCE(cd.parameter, 'NA') AS parameter_daily_max,
    COALESCE(cd.aqi_value, 0) AS aqi_daily_max,
    COALESCE(ma.aqi_monthly_avg, 0) AS aqi_monthly_avg
FROM city_month cm
LEFT JOIN city_daily cd 
    ON cd.city = cm.city 
    AND cd.month_key = cm.month_number
LEFT JOIN monthly_avg ma 
    ON ma.city = cd.city
    AND ma.month_key = cd.month_key
ORDER BY cm.city, cm.month_number, cd.date;
