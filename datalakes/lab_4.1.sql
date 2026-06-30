-- Step 1
DESCRIBE icebergS3(
    'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
    NOSIGN
);

-- Step 2
SELECT count() FROM icebergS3(
    'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
    NOSIGN
);


-- Step 3
SELECT *
FROM icebergS3('s3://learn-clickhouse/data_lakes_demo/')
LIMIT 5;

-- Step 4
SELECT
    payment_type,
    count() AS trips,
    avg(total_amount) AS avg_total,
    avg(tip_amount) AS avg_tip
FROM
(
    SELECT payment_type, total_amount, tip_amount
    FROM icebergS3(
        'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
        NOSIGN
    )
    LIMIT 3000000
)
GROUP BY payment_type
ORDER BY trips DESC;


-- Step 5
CREATE OR REPLACE TABLE nyc_taxi_mergetree
(
    vendor_id           LowCardinality(String),
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    passenger_count     UInt8,
    trip_distance       Float32,
    payment_type        LowCardinality(String),
    fare_amount         Float32,
    tip_amount          Float32,
    total_amount        Float32,
    pickup_location_id  LowCardinality(String),
    dropoff_location_id LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (payment_type, pickup_datetime, pickup_location_id);

INSERT INTO nyc_taxi_mergetree
SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    pickup_location_id,
    dropoff_location_id
FROM icebergS3(
    'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
    NOSIGN
)
LIMIT 3000000
SETTINGS insert_null_as_default = 1;



-- Step 6
SELECT COUNT() FROM nyc_taxi_mergetree;

-- Step 7
SELECT
    payment_type,
    count() AS trips,
    avg(total_amount) AS avg_total,
    avg(tip_amount) AS avg_tip
FROM nyc_taxi_mergetree
GROUP BY payment_type
ORDER BY trips DESC;
