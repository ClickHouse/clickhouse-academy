-- You can choose which size dataset to use depending on your available resources
-- SMALL: (2009-H1, 1GB) https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet
-- Medium (2009, 2.7GB): https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009.parquet
-- Medium (2010, 2.7GB): https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2010.parquet
-- FULL (10GB): https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_2009-2010.parquet
-- Edit create table statement accordingly

-- 
CREATE TABLE nyc_taxi
ENGINE = MergeTree 
ORDER BY () EMPTY
AS SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

SHOW CREATE TABLE nyc_taxi format vertical;

INSERT INTO nyc_taxi
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

INSERT INTO nyc_taxi
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/nyc_taxi_h1-2009.parquet');

-- Create and populate taxi zone lookup table
CREATE TABLE taxi_zone_lookup
(
  `id` Nullable(String),
  `borough` Nullable(String),
  `zone` Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO taxi_zone_lookup
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/clickhouse-academy/taxi_zone_lookup.csv');
