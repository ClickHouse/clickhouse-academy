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
INSERT INTO uk_prices_1
    SELECT * 
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