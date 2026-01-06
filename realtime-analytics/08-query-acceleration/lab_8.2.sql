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

-- Alternative Steps for Refreshable Materialized Views (using Atomic database) needed when using ClickHouse OSS.
-- Why we need this approach:
-- The original steps fail with: Code: 80. DB::Exception: Refreshable materialized views 
-- (except with APPEND) only support Atomic and Replicated database engines, but database 
-- default has engine Overlay. (INCORRECT_QUERY)
--
-- Refreshable materialized views require transactional guarantees that only Atomic and 
-- Replicated database engines provide. The default Overlay engine doesn't support these
-- features, so we need to create a separate database with Atomic engine.

-- Step 1: Create the Atomic database
CREATE DATABASE my_atomic_db ENGINE = Atomic;

-- Step 2: Create the destination table in the new database
CREATE TABLE my_atomic_db.uk_averages_by_day (
    day LowCardinality(String),
    average_price UInt32
)
ENGINE = MergeTree
PRIMARY KEY day;

-- Step 3: Create the refreshable materialized view
CREATE MATERIALIZED VIEW my_atomic_db.uk_averages_by_day_mv
REFRESH EVERY 12 HOURS
TO my_atomic_db.uk_averages_by_day
AS
SELECT
    toYYYYMMDD(date) AS day,
    avg(price) AS average_price
FROM uk_prices_3  -- This references the table in your default database
WHERE toYear(date) >= 2025
GROUP BY day;

-- Step 4: Check the data
SELECT * FROM my_atomic_db.uk_averages_by_day ORDER BY day;
