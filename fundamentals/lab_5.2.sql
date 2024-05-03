--Step 1:
SELECT count()
FROM crypto_prices
WHERE change_1_day > 10;

--Step 2:
CREATE TABLE big_changes (
    crypto_name String,
    trade_date Date,
    change_1_day Float32
)
ORDER BY (crypto_name, trade_date);

--Step 3:
CREATE MATERIALIZED VIEW big_changes_view
TO big_changes
AS
   SELECT 
      crypto_name,
      trade_date,
      change_1_day
    FROM crypto_prices
    WHERE change_1_day > 10;

--Step 4:
INSERT INTO big_changes
    SELECT 
       crypto_name,
       trade_date,
       change_1_day 
    FROM crypto_prices
       WHERE change_1_day > 10;

--Step 5:
SELECT count() FROM big_changes;

--Step 6:
INSERT INTO crypto_prices (crypto_name, trade_date, change_1_day) VALUES
   ('ClickHouse Coin', now(), 50);

--Step 7:
SELECT *
FROM big_changes_view 
WHERE crypto_name LIKE 'ClickHouse Coin';