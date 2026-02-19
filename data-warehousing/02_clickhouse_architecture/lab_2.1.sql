-- Step 1
CREATE DATABASE SOURCES;

-- Step 2
CREATE TABLE badges (
    id UInt32,
    user_id Int32,
    name LowCardinality(String),
    date DateTime,
    class Enum('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    tag_based Bool
)
ENGINE = MergeTree
PRIMARY KEY (name, user_id, date);

-- Step 3
INSERT INTO badges
SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/badges.parquet', 'Parquet');

-- Step 4
SELECT *
FROM system.parts
WHERE table = 'badges'
AND active = 1;

-- Step 5
SELECT
   formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
   formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'badges' AND active = 1;

-- Step 6
-- ANSWER: 3,542,651/8192 = 432.45, so there are 433 granules.

-- Step 7
SELECT *
FROM badges
WHERE class = 'Bronze';

--Step 8
SELECT *
FROM badges
WHERE toYYYYMM(date) = '202202';
-- ANSWER: Your results may vary slightly, but if your query processed 81,920 rows, notice that is exactly 8192 x 10, so 10 granules.

  
-- Step 9
CREATE TABLE badges_bad_primary_key (
    id UInt32,
    user_id Int32,
    name LowCardinality(String),
    date DateTime,
    class Enum('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    tag_based Bool
)
ENGINE = MergeTree
PRIMARY KEY (name, user_id, date);


-- Step 10
INSERT INTO badges_bad_primary_key
SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/badges.parquet', 'Parquet');


-- Step 11
-- Answer: You read 1,286,144 rows, so 157 granules. You had to do a full table scan, when before, choosing an optimal primary key allowed you to skip granules. 

-- Step 12
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'badges_bad_primary_key' AND active = 1;



