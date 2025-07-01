-- Step 1
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet');

-- Step 2
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
SETTINGS schema_inference_make_columns_nullable=0;

-- Step 3
CREATE TABLE weather_temp
ENGINE = Memory
AS
    SELECT *
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
    LIMIT 100
    SETTINGS schema_inference_make_columns_nullable=0;

-- Step 4
SHOW CREATE TABLE weather_temp;

-- Step 5
CREATE TABLE weather
(
    `station_id` LowCardinality(String),
    `date` Date32,
    `tempAvg` Int32,
    `tempMax` Int32,
    `tempMin` Int32,
    `precipitation` Int32,
    `snowfall` Int32,
    `snowDepth` Int32,
    `percentDailySun` Int8,
    `averageWindSpeed` Int32,
    `maxWindSpeed` Int32,
    `weatherType` UInt8,
    `location` Tuple(
        `1` Float64,
        `2` Float64),
    `elevation` Float32,
    `name` LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY date;

-- Step 6
INSERT INTO weather
    SELECT * 
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
    WHERE toYear(date) >= '1995';

-- Step 7
SELECT formatReadableQuantity(count()) FROM weather;

-- Step 8
SELECT
    tempMax / 10 AS maxTemp,
    location,
    name,
    date
FROM weather
WHERE tempMax > 500
ORDER BY
    tempMax DESC,
    date ASC
LIMIT 10;

-- Step 9
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'weather' AND active = 1;