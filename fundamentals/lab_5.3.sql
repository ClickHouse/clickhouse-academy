--Step 1:
CREATE MATERIALIZED VIEW market_caps_view 
ENGINE = AggregatingMergeTree()
ORDER BY (crypto_name) 
POPULATE AS
SELECT
    crypto_name,
    maxSimpleState(market_cap) AS max_market_cap,
    minSimpleState(market_cap) AS min_market_cap
FROM
    crypto_prices
WHERE
    crypto_name != ''
GROUP BY
    crypto_name;

--Step 2:
SELECT count() FROM market_caps_view;

--Step 3:
SELECT * 
FROM market_caps_view
ORDER BY max_market_cap DESC;

--Step 4:
SELECT
   max(market_cap),
   min(market_cap)
FROM crypto_prices
WHERE crypto_name = 'Bitcoin';

SELECT
    max_market_cap,
    min_market_cap
FROM
    market_caps_view
WHERE
    crypto_name = 'Bitcoin';

--Step 5:
INSERT INTO crypto_prices VALUES
   ('2022-12-01', 'Bitcoin', 123, 0.0, 0.0, -1);

--Step 6:
SELECT
    max_market_cap,
    min_market_cap
FROM
    market_caps_view
WHERE
    crypto_name = 'Bitcoin';

--Step 7:
SELECT
    max_market_cap,
    min_market_cap
FROM
    market_caps_view FINAL
WHERE
    crypto_name = 'Bitcoin';