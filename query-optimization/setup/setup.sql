/*
 * Initial oad into nyc_taxi and taxi_zone_lookup
 */

CREATE OR REPLACE TABLE nyc_taxi
ENGINE = MergeTree 
ORDER BY () EMPTY
AS SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

INSERT INTO nyc_taxi
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

CREATE OR REPLACE TABLE taxi_zone_lookup
(
  `id` Nullable(String),
  `borough` Nullable(String),
  `zone` Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO taxi_zone_lookup
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');

/*
 * Load nyc_taxi_opt3
 */

CREATE OR REPLACE TABLE nyc_taxi_opt3
(
    `vendor_id` String,
    `pickup_datetime` DateTime64(6, 'UTC'),
    `dropoff_datetime` DateTime64(6, 'UTC'),
    `passenger_count` Int64,
    `trip_distance` Float64,
    `ratecode_id` String,
    `pickup_location_id` String,
    `dropoff_location_id` String,
    `payment_type` Int64,
    `fare_amount` Float64,
    `extra` Float64,
    `mta_tax` Float64,
    `tip_amount` Float64,
    `tolls_amount` Float64,
    `total_amount` Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO nyc_taxi_opt3
SELECT * FROM nyc_taxi;

/*
 * Load nyc_taxi_opt4_1
 */

CREATE OR REPLACE TABLE nyc_taxi_opt4_1
(
    `vendor_id` UInt8,
    `pickup_datetime` DateTime,
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Decimal32(2),
    `ratecode_id` LowCardinality(String),
    `pickup_location_id` LowCardinality(String),
    `dropoff_location_id` LowCardinality(String),
    `payment_type` UInt8,
    `fare_amount` Decimal32(2),
    `extra` Decimal32(2),
    `mta_tax` Decimal32(2),
    `tip_amount` Decimal32(2),
    `tolls_amount` Decimal32(2),
    `total_amount` Decimal32(2)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO nyc_taxi_opt4_1
SELECT * FROM nyc_taxi_opt3;

/*
 * Load nyc_taxi_opt4_2 and taxi_zone_lookup_opt4_2
 */

CREATE OR REPLACE TABLE nyc_taxi_opt4_2
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
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO nyc_taxi_opt4_2
SELECT * FROM nyc_taxi_opt3;

CREATE OR REPLACE TABLE taxi_zone_lookup_opt4_2
(
    `id` UInt16,
    `borough` Nullable(String),
    `zone` Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO taxi_zone_lookup_opt4_2
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');

/*
 * Load nyc_taxi_opt5_1
 */

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

INSERT INTO nyc_taxi_opt5_1 SELECT * FROM nyc_taxi_opt4_2;

/*
 * Load nyc_taxi_opt5_2
 */

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

INSERT INTO nyc_taxi_opt5_2 SELECT * FROM nyc_taxi_opt4_2;

/*
 * Load nyc_taxi_opt5_3
 */

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

INSERT INTO nyc_taxi_opt5_3 SELECT * FROM nyc_taxi_opt4_2;

/*
 * Load nyc_taxi_opt5_4
 */

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

INSERT INTO nyc_taxi_opt5_4 SELECT * FROM nyc_taxi_opt4_2;

/*
 * Load nyc_taxi_opt5_5
 */

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

INSERT INTO nyc_taxi_opt5_5 SELECT * FROM nyc_taxi_opt4_2;

/*
 * Load taxi_zone_lookup_opt6_1
 */

CREATE OR REPLACE TABLE taxi_zone_lookup_opt6_1
(
  `id` UInt16,
  `borough` LowCardinality(String),
  `zone` LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (id, borough, zone);

INSERT INTO taxi_zone_lookup_opt6_1
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');

/*
 * Load taxi_zone_lookup_opt6_2
 */

CREATE OR REPLACE DICTIONARY taxi_zone_lookup_opt6_2
(
    `id` UInt64,
    `borough` String,
    `zone` String
)
PRIMARY KEY id
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(3600)
LAYOUT(FLAT());

/*
 * Load nyc_taxi_opt7
 */

CREATE OR REPLACE TABLE nyc_taxi_opt7
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
    `ride_date` Date MATERIALIZED toDate(pickup_datetime),
    `trip_time` UInt32 MATERIALIZED dateDiff('s', pickup_datetime, dropoff_datetime)::UInt32
)
ENGINE = MergeTree
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime);

INSERT INTO nyc_taxi_opt7 SELECT * FROM nyc_taxi_opt5_1;

/*
 * Load nyc_taxi_opt8
 */

CREATE OR REPLACE TABLE nyc_taxi_opt8
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
    PROJECTION proj_trip_distance_pickup_datetime
    (
        SELECT vendor_id, total_amount, trip_distance, trip_time
        ORDER BY (trip_distance, trip_time, vendor_id, total_amount)
    ),
)
ENGINE = MergeTree
PRIMARY KEY (payment_type, passenger_count, pickup_datetime, dropoff_datetime)
SETTINGS index_granularity = 8192;

INSERT INTO nyc_taxi_opt8 SELECT * FROM nyc_taxi_opt7;

/*
 * Load materialized view and AggregatingMeerteTree
 */

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
FROM nyc_taxi_opt5_1
GROUP BY pickup_location_id;

INSERT INTO nyc_taxi_pickup_location_id_trips_DEST
SELECT
    pickup_location_id, countState()
FROM nyc_taxi_opt5_1
GROUP BY pickup_location_id;
