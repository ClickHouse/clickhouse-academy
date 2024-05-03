--Step 1:
SELECT count()
FROM crypto_raw
WHERE USD_price_change_1_day > 10;

--Step 2:
SELECT
    crypto_name,
    max(market_cap) AS max_market_cap
FROM crypto_raw
GROUP BY crypto_name
ORDER BY max_market_cap DESC
LIMIT 10;

--Step 3:
SELECT
    argMax(crypto_name, price_usd) AS crypto_name,
    toStartOfMonth(trade_date) AS month,
    max(price_usd)
FROM crypto_raw
WHERE trade_date >= toDate('2017-01-01') 
  AND trade_date <= toDate('2018-12-31')
GROUP BY month;

--Step 4:
SELECT 
    avg(volume),
    avgIf(volume, crypto_name != '')
FROM crypto_raw;

--Step 5:
SELECT uniqExact(crypto_name) 
FROM crypto_raw;