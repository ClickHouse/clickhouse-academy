-- Comparing different primary key configurations

-- Step 5_1: PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

CREATE OR REPLACE TABLE nyc_taxi_opt5_1
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_opt5_1
SELECT * FROM nyc_taxi_opt4_2;

-- 5_2: PRIMARY KEY (pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime);

CREATE OR REPLACE TABLE nyc_taxi_opt5_2
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
PRIMARY KEY (pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_opt5_2
SELECT * FROM nyc_taxi_opt4_2;

-- 5_3: PRIMARY KEY (pickup_location_id, passenger_count, pickup_datetime)

CREATE OR REPLACE TABLE nyc_taxi_opt5_3
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
PRIMARY KEY (pickup_location_id, passenger_count, pickup_datetime);

INSERT INTO nyc_taxi_opt5_3
SELECT * FROM nyc_taxi_opt4_2;

-- Step 5_4: PRIMARY KEY (trip_distance, pickup_datetime)
CREATE OR REPLACE TABLE nyc_taxi_opt5_4
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
PRIMARY KEY (trip_distance, pickup_datetime);

INSERT INTO nyc_taxi_opt5_4
SELECT * FROM nyc_taxi_opt4_2;

-- 5_5: PRIMARY KEY (pickup_datetime);
CREATE OR REPLACE TABLE nyc_taxi_opt5_5
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
PRIMARY KEY (pickup_datetime);

INSERT INTO nyc_taxi_opt5_5
SELECT * FROM nyc_taxi_opt4_2;