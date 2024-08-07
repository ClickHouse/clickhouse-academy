-- Step 1
DESCRIBE gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet');

-- Step 2
SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet')
FORMAT Vertical;

-- Step 4
-- The UserId, Name, and Date columns are all great candidates

-- Step 5
-- The Name column has the lowest cardinality, so it may as well be first, followed by UserId, then Date (which is often at the end of a primary key)

-- Step 6
CREATE TABLE badges (
    Id UInt32,
    UserId Int32,
    Name LowCardinality(String),
    Date DateTime,
    Class Enum('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    TagBased Bool
)
ENGINE = MergeTree
PRIMARY KEY (Name, UserId, Date);

-- Step 7
INSERT INTO badges
SELECT * FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet');

-- Step 8
SELECT count() FROM badges;

-- Step 9
SELECT * FROM badges LIMIT 1000;