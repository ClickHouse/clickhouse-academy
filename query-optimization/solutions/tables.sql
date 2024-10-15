-- 
-- DROP TABLE query_training;
CREATE DATABASE query_training;
USE query_training;

-- Create taxi data table with inferred schema

CREATE OR REPLACE TABLE nyc_taxi_inferred
ENGINE = MergeTree 
ORDER BY () EMPTY
AS SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet');

-- Insert small dataset
INSERT INTO nyc_taxi_inferred SELECT * FROM s3Cluster('default','https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

-- Insert medium dataset
-- INSERT INTO nyc_taxi_inferred SELECT * FROM s3Cluster('default','https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet');

-- Insert full dataset
--INSERT INTO nyc_taxi_inferred SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009-2010.parquet');


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
ENGINE = MergeTree
ORDER BY tuple();

-- Insert from existing
INSERT INTO nyc_taxi SELECT * FROM nyc_taxi_inferred;

-- Creaet and populate lookup table with inferred schema

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
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO taxi_zone_lookup
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');



-- Try out various primary keys

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
ENGINE = MergeTree
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_key_1 SELECT * FROM nyc_taxi;

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
ENGINE = MergeTree
PRIMARY KEY (pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_key_2 SELECT * FROM nyc_taxi;


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
ENGINE = MergeTree
PRIMARY KEY (pickup_location_id, passenger_count, pickup_datetime);

INSERT INTO nyc_taxi_key_3 SELECT * FROM nyc_taxi;


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
ENGINE = MergeTree
PRIMARY KEY (trip_distance, pickup_datetime);

INSERT INTO nyc_taxi_key_4 SELECT * FROM nyc_taxi;


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
ENGINE = MergeTree
PRIMARY KEY (pickup_datetime);

INSERT INTO nyc_taxi_key_5 SELECT * FROM nyc_taxi;




-- Replace lookup table with dictionary

RENAME TABLE taxi_zone_lookup to taxi_zone_lookup_table

CREATE DICTIONARY taxi_zone_lookup
(
    `id` UInt64,
    `borough` String,
    `zone` String
)
PRIMARY KEY id
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(3600)
LAYOUT(FLAT());

