--Step 1:
DESCRIBE s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

--Step 2:
CREATE TABLE crypto_prices (
   trade_date Date,
   crypto_name LowCardinality(String),
   volume Float32,
   price Float32,
   market_cap Float32,
   change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

--Step 3:
INSERT INTO crypto_prices
   SELECT *
   FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

--Step 4:
SELECT count()
FROM crypto_prices;

--Step 5:
SELECT count()
FROM crypto_prices
WHERE volume >= 1_000_000;

/*
 * It read all of the rows because volume is not part of the primary key.
 */

--Step 6:
SELECT
   avg(price)
FROM crypto_prices
WHERE crypto_name = 'Bitcoin';

/*
 * Only a very small number of granules was processed. As crypto_name is a
 * primary key, ClickHouse use it to optmize the query.
 */

--Step 7:
SELECT
   avg(price)
FROM crypto_prices
WHERE crypto_name LIKE 'B%';
