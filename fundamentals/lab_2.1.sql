--Step 1:
SELECT * 
FROM s3(
   'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_raw.csv.gz',
   'CSVWithNames'
)
LIMIT 100;

--Step 2:
SELECT *
FROM s3(
   'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_raw.csv.gz',
   'CSVWithNames'
);

--Step 3:
SELECT *
FROM s3(
   'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_raw.csv.gz',
   'CSVWithNames'
)
SETTINGS input_format_allow_errors_num=100;

--Step 4:
SELECT *
FROM s3(
   'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_raw.csv.gz',
   'CSVWithNames'
)
LIMIT 50000
SETTINGS schema_inference_hints='volume Float32, market_cap Float32';

--Step 5:
CREATE TABLE crypto_raw (
    trade_date Date,
    volume Float32,
    price_usd Float32,
    price_btc Float32,
    market_cap Float32,
    capitalization_change_1_day Float32,
    USD_price_change_1_day Float32,
    BTC_price_change_1_day Float32,
    crypto_name LowCardinality(String),
    crypto_type UInt8,
    ticker LowCardinality(String),
    max_supply Float32,
    site_url LowCardinality(String),
    github_url LowCardinality(String),
    minable UInt8,
    platform_name LowCardinality(String),
    industry_name LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

--Step 6:
INSERT INTO crypto_raw 
   SELECT *
   FROM s3(
      'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_raw.csv.gz',
      'CSVWithNames'
   )
   SETTINGS schema_inference_hints='volume Float32, market_cap Float32';

--Step 7:
SELECT 
   crypto_name,
   count() AS count
FROM crypto_raw
GROUP BY crypto_name
ORDER BY crypto_name;

--Step 8:
SELECT 
   crypto_name,
   max(price_usd) AS m,
   max(price_btc)
FROM crypto_raw
GROUP BY crypto_name
ORDER BY m DESC;

--Step 9:
SELECT 
   platform_name,
   uniq(crypto_name) AS count
FROM crypto_raw
WHERE platform_name != ''
GROUP BY platform_name
ORDER BY count DESC;

--Step 10:
SELECT 
   crypto_name,
   count() AS count
FROM crypto_raw
WHERE USD_price_change_1_day < 0
AND crypto_name != ''
GROUP BY crypto_name
ORDER BY count DESC;

--Step 11:
SELECT 
   crypto_name,
   count() AS count
FROM crypto_raw
WHERE USD_price_change_1_day < -0.5
AND crypto_name != ''
GROUP BY crypto_name
ORDER BY count DESC;