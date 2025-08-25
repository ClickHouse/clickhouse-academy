-- Step 1
CREATE TABLE uk_averages_by_day (
    day LowCardinality(String),
    average_price UInt32
)
ENGINE = MergeTree
PRIMARY KEY day;

CREATE MATERIALIZED VIEW uk_averages_by_day_mv
REFRESH EVERY 12 HOURS
TO uk_averages_by_day
AS
    SELECT
        toYYYYMMDD(date) AS day,
        avg(price) AS average_price
    FROM uk_prices_3
    WHERE toYear(date) >= '2025'
    GROUP BY day;

-- Step 2
SELECT * 
FROM uk_averages_by_day;