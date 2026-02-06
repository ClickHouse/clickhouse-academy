-- Step 8
DESCRIBE TABLE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/badges.parquet', 'Parquet');

-- Step 9
SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/badges.parquet', 'Parquet'); LIMIT 100;

-- Step 10
CREATE TABLE uk_prices
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

INSERT INTO uk_prices
    SELECT * 
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

SELECT * from uk_prices;

