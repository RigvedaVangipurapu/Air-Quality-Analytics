CREATE TABLE d_city (
    city_name VARCHAR(100) PRIMARY KEY,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

INSERT INTO d_city (city_name, latitude, longitude) VALUES
    ('New York City, NY', 40.7128, -74.0060),
    ('Los Angeles, CA', 34.0522, -118.2437),
    ('Chicago, IL', 41.8781, -87.6298),
    ('Houston, TX', 29.7604, -95.3698),
    ('Phoenix, AZ', 33.4484, -112.0740),
    ('Philadelphia, PA', 39.9526, -75.1652),
    ('San Antonio, TX', 29.4241, -98.4936),
    ('San Diego, CA', 32.7157, -117.1611),
    ('Dallas, TX', 32.7767, -96.7970),
    ('San Jose, CA', 37.3382, -121.8863);