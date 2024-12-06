/** 
 ** Query Optimization 
 **/

-- Datasets:
-- Small -- first half of 2009 (use this one in class):
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet
-- medium -- all of 2009
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet
-- large -- 2009 and 2010
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009-2010.parquet

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

/** 
 ** Using the benchmark tool
 **/

/*
# Set up credentials to access ClickHouse service
source lab_optimization/clickhouse_env.sh

# run benchmark with Query 1 specified on command line
clickhouse benchmark --host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE \
--secure \
--enable_filesystem_cache=0 \
--iterations=5 \
--delay=0 \
--query='SELECT avg(dateDiff('s', pickup_datetime,dropoff_datetime)) FROM nyc_taxi'

# run benchmark with Query 1 from a file
./clickhouse benchmark --host=$CLICKHOUSE_HOST \
--password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 \
--delay=0 \
< lab_optimization/query_01.sql 
*/

/**
** Direct joins with Dictionaries
**/

/* Run benchmark before 
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/query_06.sql
*/

-- Create and populate zone lookup table 
CREATE OR REPLACE TABLE taxi_zone_lookup
(
  id UInt16,
  borough String,
  zone String
)
ORDER BY tuple();

INSERT INTO taxi_zone_lookup
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');

-- confirm insert
SELECT * FROM taxi_zone_lookup;

-- join main table with lookup to count taxi rides ending at airports
SELECT count() 
FROM nyc_taxi 
JOIN taxi_zone_lookup 
ON nyc_taxi.dropoff_location_id = taxi_zone_lookup.id 
WHERE borough='Queens';

-- Always disable filesystem caching when testing optimizations
SET enable_filesystem_cache = 0;


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

-- confirm lookup dictionary works the same
SELECT * FROM taxi_zone_lookup;

/* 
# Benchmark solution with dictGet direct join 
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_06_dictionary.sql
*/

/** 
 ** Insert-time computation with materialized columns 
 **/

/*
# benchmark query 1 before
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/query_01.sql
*/

-- Create a new taxi data table with a materialized column for trip time
CREATE TABLE nyc_taxi_with_trip_time
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
    total_amount Decimal32(2),
    trip_time UInt32 MATERIALIZED dateDiff('s', pickup_datetime, dropoff_datetime)::UInt32
)
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_with_trip_time SELECT * FROM nyc_taxi_key_1;

/*
# benchmark with materialized trip time column
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_01_materialized_trip_time.sql
*/

-- add materialized column to compute trip_time when new rows are inserted
ALTER TABLE nyc_taxi_with_trip_time
ADD COLUMN ride_date Date MATERIALIZED toDate(pickup_datetime);

-- force computation of the column for existing rows
ALTER TABLE nyc_taxi_with_trip_time MATERIALIZE COLUMN ride_date;

/*
# benchmark with materialized trip time column
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/query_09.sql

./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_09_materialized_ride_date.sql
*/

/** 
 ** Projections 
 **/

-- create table with projection
CREATE OR REPLACE TABLE nyc_taxi_projection (
    vendor_id UInt8,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Decimal(9, 2),
    ratecode_id LowCardinality(String),
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    payment_type UInt8,
    fare_amount Decimal(9, 2),
    extra Decimal(9, 2),
    mta_tax Decimal(9, 2),
    tip_amount Decimal(9, 2),
    tolls_amount Decimal(9, 2),
    total_amount Decimal(9, 2),
    ride_date Date MATERIALIZED toDate(pickup_datetime),
    trip_time UInt32 MATERIALIZED dateDiff('s', pickup_datetime, dropoff_datetime)::UInt32,
    PROJECTION trip_distance_projection
    (
        SELECT total_amount, trip_distance
        ORDER BY (trip_distance)
    ),
)
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_projection SELECT * FROM nyc_taxi;

/*
# compare benchmark with and without projection
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/query_04.sql

./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_04_projection.sql
*/

-- Compare the number of rows and size of table with and without projections

SELECT
  table,
  sum(rows) as rows,
  sum(data_compressed_bytes) as compressed_bytes,
  sum(data_uncompressed_bytes) as uncompressed_bytes
FROM system.parts
WHERE (active = 1) AND (table LIKE 'nyc_taxi%') AND (database = currentDatabase())
GROUP BY table;

/** 
 ** Aggregating data before joining
 **/

/*
# benchmark with original query against updated
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/query_03.sql

./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_03_aggregate_first.sql
*/

/** 
 ** Materialized View with Aggregating Merge Tree 
 **/

-- create destination table for materialized view
CREATE OR REPLACE TABLE nyc_taxi_pickup_location_id_trips_DEST
(
    pickup_location_id UInt16,
    number_of_trips AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree 
PRIMARY KEY pickup_location_id;

-- Create materialized view to aggregate number of trips
-- (Drop MV first if needed -- there's no CREATE OR REPLACE equivalent for views)
DROP TABLE IF EXISTS nyc_taxi_pickup_location_id_trips_MV;

CREATE MATERIALIZED VIEW nyc_taxi_pickup_location_id_trips_MV
TO nyc_taxi_pickup_location_id_trips_DEST
AS 
SELECT
   pickup_location_id,
   countState() as number_of_trips
FROM nyc_taxi_key_1
GROUP BY pickup_location_id;

-- Manually copy historical taxi data to destination data
-- (New data inserts into source table will be imported automatically)
INSERT INTO nyc_taxi_pickup_location_id_trips_DEST
SELECT
    pickup_location_id, countState()
FROM nyc_taxi_key_1
GROUP BY pickup_location_id;

/*
# benchmark query 3 with materialized view
./clickhouse benchmark \
--host=$CLICKHOUSE_HOST --password=$CLICKHOUSE_PASSWORD \
--database=$CLICKHOUSE_DATABASE --secure \
--enable_filesystem_cache=0 \
--iterations=5 --delay=0 \
< lab_optimization/solutions/query_03_materialized_view.sql
*/

-- Compare the number of rows and size of the new table with prior ride data tables.
SELECT
  table,
  sum(rows) as rows,
  sum(data_compressed_bytes) as compressed_bytes,
  sum(data_uncompressed_bytes) as uncompressed_bytes
FROM system.parts
WHERE (active = 1) AND (table LIKE 'nyc_taxi%') AND (database = currentDatabase())
GROUP BY table;

-- look at project part metadata
SELECT
  table,
  sum(data_compressed_bytes) as compressed_bytes,
  sum(data_uncompressed_bytes) as uncompressed_bytes
FROM system.projection_parts
WHERE (active = 1) AND (table LIKE 'nyc_taxi%') AND (database = currentDatabase())
GROUP BY table;

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

