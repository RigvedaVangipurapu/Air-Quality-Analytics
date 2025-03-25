DROP TABLE IF EXISTS sensor_scd;
CREATE TABLE sensor_scd (
    sensor_id TEXT,
    is_active BOOLEAN,
    date INTEGER,
    PRIMARY KEY (sensor_id, date)
);

WITH sensor_dates AS (
    SELECT DISTINCT
        sensor_id,
        LEFT(date, 10)::DATE AS date_key
    FROM f_city_sensor_pollutant_data
),
previous_date AS (
    SELECT 
        sensor_id,  
        date_key,
        LAG(date_key) OVER (PARTITION BY sensor_id ORDER BY date_key) AS previous_date_key
    FROM sensor_dates
),
indicators AS (
SELECT
    *,
    CASE 
        WHEN previous_date_key IS NULL THEN 1  -- Correct NULL check
        WHEN (date_key - previous_date_key) = 1 THEN 0  -- Date subtraction returns an integer
        ELSE 1
    END AS change_indicator
FROM previous_date
),
streaks AS
(
    SELECT *,
    SUM(change_indicator) OVER (PARTITION BY sensor_id ORDER BY date_key)  AS streak
    FROM indicators
)
SELECT  
    sensor_id,
    MIN(date_key) AS start_date,
    MAX( date_key) AS end_date
FROM streaks
GROUP BY
    sensor_id, streak

ORDER BY 
    sensor_id, start_date;


