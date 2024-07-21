-- Step 1
SHOW CREATE TABLE postgres_badges FORMAT Vertical;

-- Step 2
SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM postgres_badges FORMAT Vertical;

-- Step 4
-- The user_id, name, and date columns are all great candidates

-- Step 5
-- The name column has the lowest cardinality, so it may as well be first, followed by user_id, then date (which is often at the end of a primary key)

-- Step 6
SELECT DISTINCT class FROM postgresql('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

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

-- Step 7
INSERT INTO badges
SELECT * FROM postgres_badges;

-- Step 8
SELECT count() FROM badges;

-- Step 9
SELECT * FROM badges LIMIT 1000;