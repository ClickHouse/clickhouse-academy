-- 
-- DROP DATABASE training;
CREATE DATABASE training;
USE training;

-- Create taxi data table with inferred schema

CREATE OR REPLACE TABLE nyc_taxi_inferred
ORDER BY () EMPTY
AS SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet');

-- Insert small dataset
INSERT INTO nyc_taxi_inferred SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

-- Insert medium dataset
-- INSERT INTO nyc_taxi_inferred SELECT * FROM s3Cluster('default','https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet');

-- Insert full dataset
-- INSERT INTO nyc_taxi_inferred SELECT * FROM s3Cluster('default','https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009-2010.parquet');


-- Create schema with proper data types

CREATE OR REPLACE TABLE nyc_taxi
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
ORDER BY tuple();

-- Insert from existing
INSERT INTO nyc_taxi SELECT * FROM nyc_taxi_inferred;

-- Create and populate lookup table with inferred schema

-- CREATE OR REPLACE TABLE taxi_zone_lookup_inferred
-- ENGINE = MergeTree 
-- ORDER BY () EMPTY
-- AS SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');

-- INSERT INTO taxi_zone_lookup_inferred
-- SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');


-- Create and populate lookup table with corrected schema
CREATE OR REPLACE TABLE taxi_zone_lookup
(
  `id` UInt16,
  `borough` String,
  `zone` String
)
ORDER BY tuple();

INSERT INTO taxi_zone_lookup
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');



-- Try out various primary keys

-- PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime)
CREATE OR REPLACE TABLE nyc_taxi_key_1
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
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_key_1 SELECT * FROM nyc_taxi;


-- PRIMARY KEY (pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime);
CREATE OR REPLACE TABLE nyc_taxi_key_2
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
PRIMARY KEY (pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_key_2 SELECT * FROM nyc_taxi;

-- Key 3: PRIMARY KEY (pickup_location_id, passenger_count, pickup_datetime);
CREATE OR REPLACE TABLE nyc_taxi_key_3
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
PRIMARY KEY (pickup_location_id, passenger_count, pickup_datetime);

INSERT INTO nyc_taxi_key_3 SELECT * FROM nyc_taxi;

-- Key 4: PRIMARY KEY (trip_distance, pickup_datetime)
CREATE OR REPLACE TABLE nyc_taxi_key_4
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
PRIMARY KEY (trip_distance, pickup_datetime);

INSERT INTO nyc_taxi_key_4 SELECT * FROM nyc_taxi;

-- Key 5: PRIMARY KEY (pickup_datetime)
CREATE OR REPLACE TABLE nyc_taxi_key_5
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
PRIMARY KEY (pickup_datetime);

INSERT INTO nyc_taxi_key_5 SELECT * FROM nyc_taxi;




-- Replace lookup table with dictionary

RENAME TABLE taxi_zone_lookup TO taxi_zone_lookup_table;

CREATE OR REPLACE DICTIONARY taxi_zone_lookup
(
    `id` UInt64,
    `borough` String,
    `zone` String
)
PRIMARY KEY id
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(3600)
LAYOUT(FLAT());

CREATE OR REPLACE TABLE nyc_taxi_with_trip_time
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
    `total_amount` Decimal32(2),
    `trip_time` UInt32 MATERIALIZED dateDiff('s', pickup_datetime, dropoff_datetime)::UInt32
)
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_with_trip_time SELECT * FROM nyc_taxi_key_1;


-- compute trip_time when new rows are inserted
ALTER TABLE nyc_taxi_with_trip_time
ADD COLUMN ride_date Date MATERIALIZED toDate(pickup_datetime);

-- force computation of the column for existing rows
ALTER TABLE nyc_taxi_with_trip_time MATERIALIZE COLUMN ride_date;


-- Bonus -- test materialization impact on insertion time


CREATE OR REPLACE TABLE nyc_taxi_test_insert AS nyc_taxi_key_1;
CREATE OR REPLACE TABLE nyc_taxi_test_insert_materialized AS nyc_taxi_with_trip_time;

INSERT INTO nyc_taxi_test_insert SELECT * FROM nyc_taxi_key_1
INSERT INTO nyc_taxi_test_insert_materialized SELECT * FROM nyc_taxi_key_1

-- create table with projection
CREATE OR REPLACE TABLE nyc_taxi_projection
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal(9, 2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `payment_type` UInt8,
    `fare_amount` Decimal(9, 2),
    `extra` Decimal(9, 2),
    `mta_tax` Decimal(9, 2),
    `tip_amount` Decimal(9, 2),
    `tolls_amount` Decimal(9, 2),
    `total_amount` Decimal(9, 2),
    `ride_date` Date MATERIALIZED toDate(pickup_datetime),
    `trip_time` UInt32 MATERIALIZED dateDiff('s', pickup_datetime, dropoff_datetime)::UInt32,
    PROJECTION trip_distance_amount_projection
    (
        SELECT total_amount, trip_distance
        ORDER BY (trip_distance)
    ),
)
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_projection SELECT * FROM nyc_taxi_with_trip_time;


-- Load materialized view and AggregatingMergeTree

CREATE OR REPLACE TABLE nyc_taxi_pickup_location_id_trips_DEST
(
    pickup_location_id UInt16,
    number_of_trips AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree 
PRIMARY KEY pickup_location_id;

DROP TABLE IF EXISTS nyc_taxi_pickup_location_id_trips_MV;

CREATE MATERIALIZED VIEW nyc_taxi_pickup_location_id_trips_MV
TO nyc_taxi_pickup_location_id_trips_DEST
AS 
SELECT
   pickup_location_id,
   countState() as number_of_trips
FROM nyc_taxi_key_1
GROUP BY pickup_location_id;

INSERT INTO nyc_taxi_pickup_location_id_trips_DEST
SELECT
    pickup_location_id, countState()
FROM nyc_taxi_key_1
GROUP BY pickup_location_id;

-- Bonus: Measure overhead at insert time using an aggregated materialized view

CREATE OR REPLACE TABLE nyc_taxi_test_insert AS nyc_taxi_key_1;
CREATE OR REPLACE TABLE nyc_taxi_test_insert_materialized AS nyc_taxi_with_trip_time;

CREATE OR REPLACE TABLE nyc_taxi_test_insert_materialized_DEST
(
    pickup_location_id UInt16,
    number_of_trips AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree 
PRIMARY KEY pickup_location_id;

DROP TABLE IF EXISTS nyc_taxi_test_insert_materialized_MV;

CREATE MATERIALIZED VIEW nyc_taxi_test_insert_materialized_MV
TO nyc_taxi_test_insert_materialized_DEST
AS 
SELECT
   pickup_location_id,
   countState() as number_of_trips
FROM nyc_taxi_test_insert_materialized
GROUP BY pickup_location_id;

INSERT INTO nyc_taxi_test_insert_materialized SELECT * FROM nyc_taxi_key_1;

INSERT INTO nyc_taxi_test_insert SELECT * FROM nyc_taxi_key_1;

