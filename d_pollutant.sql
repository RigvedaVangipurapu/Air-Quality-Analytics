CREATE TABLE d_pollutant (
    pollutant_name VARCHAR(10) PRIMARY KEY,
    unit VARCHAR(10) NOT NULL,
    regulatory_threshold DECIMAL(10,2) -- Allows floating point values
);

INSERT INTO d_pollutant (pollutant_name, unit, regulatory_threshold) VALUES
    ('O3', 'ppm', 0.070),    -- 70 ppb = 0.070 ppm (8-hour avg, EPA)
    ('NO2', 'ppm', 0.100),   -- 100 ppb = 0.100 ppm (1-hour avg, EPA)
    ('PM10', 'µg/m³', 150.00), -- 150 µg/m³ (24-hour avg, EPA)
    ('SO2', 'ppm', 0.075),   -- 75 ppb = 0.075 ppm (1-hour avg, EPA)
    ('CO', 'ppm', 9.00),     -- 9 ppm (8-hour avg, EPA)
    ('BC', 'µg/m³', NULL),   -- No standard regulatory threshold for BC
    ('PM25', 'µg/m³', 35.00); -- 35 µg/m³ (24-hour avg, EPA)
;