-- Step 1
SELECT * 
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst')
LIMIT 1000;

-- Step 2
DESC s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

-- Step 3
CREATE OR REPLACE TABLE uk_prices_temp 
ENGINE = Memory
AS 
    SELECT * 
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst')
    LIMIT 100;

-- Step 4
SHOW CREATE TABLE uk_prices_temp;

-- Step 5
CREATE TABLE uk_prices_1
(
    `id` Nullable(String),
    `price` Nullable(String),
    `date` DateTime,
    `postcode` Nullable(String),
    `type` Nullable(String),
    `is_new` Nullable(String),
    `duration` Nullable(String),
    `addr1` Nullable(String),
    `addr2` Nullable(String),
    `street` Nullable(String),
    `locality` Nullable(String),
    `town` Nullable(String),
    `district` Nullable(String),
    `county` Nullable(String),
    `column15` Nullable(String),
    `column16` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY date;

-- Step 6
-- If you are using Clickhouse Cloud, run this query to insert your data into uk_prices_1
INSERT INTO uk_prices_1
    SELECT * 
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

-- If you are using the Clickhouse OSS, run this query instead
NSERT INTO uk_prices_1
SELECT
    c1 as id,                 
    c2 as price,        
    parseDateTimeBestEffortOrNull(c3) AS date,
    c4 as postcode,     
    c5 as type,         
    c6 as is_new,      
    c7 as duration,    
    c8 as addr1,
    c9 as addr2,
    c10 as street,
    c11 as locality,
    c12 as town,
    c13 as district,
    c14 as county,
    c15 as column15,
    c16 as column16
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

-- Step 7
SELECT count() FROM uk_prices_1;

-- Step 8
SELECT avg(toUInt32(price))
FROM uk_prices_1;

-- Step 9
SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE toYear(date) >= '2020';

-- Step 10
SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE town = 'LONDON';
