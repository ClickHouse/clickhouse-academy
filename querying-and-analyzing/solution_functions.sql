-- Datasets:
-- first half of 2009 
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet

-- Create schema table for taxi ride data

CREATE OR REPLACE TABLE nyc_taxi
(
    vendor_id UInt8,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Decimal32(2),
    ratecode_id LowCardinality(String),
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    payment_type UInt8,
    fare_amount Decimal32(2),
    extra Decimal32(2),
    mta_tax Decimal32(2),
    tip_amount Decimal32(2),
    tolls_amount Decimal32(2),
    total_amount Decimal32(2)
)
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

-- Insert data set
INSERT INTO nyc_taxi 
SELECT * FROM
s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

-- Review one row of data
SELECT * 
FROM nyc_taxi 
LIMIT 1 
FORMAT VERTICAL;

-- display the number of rows as a human-readable number
SELECT formatReadableQuantity(count()) FROM nyc_taxi;

-- find the pickup location with highest fare
SELECT argMax(fare_amount,pickup_location_id) FROM nyc_taxi;

-- find the exact number of unique values in the pickup_datetime column.
SELECT uniqExact(pickup_datetime) FROM nyc_taxi;


-- List up to 500 rides that started on Monday (day=1)
SELECT 
    toDayOfWeek(pickup_datetime) AS day, 
    total_amount 
FROM nyc_taxi 
WHERE day=1
LIMIT 500;

-- Show the average total amount by day of the week
SELECT 
    toDayOfWeek(pickup_datetime) AS day, 
    avg(total_amount) 
FROM nyc_taxi 
GROUP BY day; 

-- Show the number of rides for each week
SELECT 
    toStartOfWeek(pickup_datetime) AS week, 
    count() 
FROM nyc_taxi 
GROUP BY week;

