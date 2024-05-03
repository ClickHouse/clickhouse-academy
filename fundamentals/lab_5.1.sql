--Step 1:
DROP TABLE IF EXISTS crypto_prices;

CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT * 
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

--Step 2:
CREATE VIEW interesting_crypto_prices AS
SELECT
    *
FROM
    crypto_prices
WHERE
    crypto_name != ''
    AND market_cap > 0;

--Step 3:
SELECT count() FROM interesting_crypto_prices;

--Step 4:
SOLUTION:

SELECT
    crypto_name,
    max(market_cap) AS max_market_cap,
    min(market_cap) AS min_market_cap
FROM
    interesting_crypto_prices
GROUP BY
    crypto_name
ORDER BY
    max_market_cap DESC;