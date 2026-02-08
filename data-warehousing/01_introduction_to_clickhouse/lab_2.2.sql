-- Step 1
CREATE OR REPLACE TABLE users_good_partition (
    user_id Int32,
    reputation Int32,
    creation_date DateTime,
    display_name String,
    last_access_date DateTime,
    website_url String,
    location String,
    about_me String,
    views Int32,
    up_votes Int32,
    down_votes Int32,
    account_id Int32
) ENGINE = MergeTree()
PARTITION BY toYear(creation_date)  -- Year instead of month!
ORDER BY (creation_date, user_id);

INSERT INTO users_good_partition
SELECT 
    id AS user_id,
    reputation,
    creation_date,
    display_name,
    last_access_date,
    website_url,
    location,
    about_me,
    views,
    up_votes,
    down_votes,
    account_id
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet');


-- Step 2
SELECT 
count() AS total_parts,
uniqExact(partition) AS total_partitions,
sum(rows) AS total_rows,
formatReadableSize(sum(bytes_on_disk)) AS total_size
FROM system.parts
WHERE table = 'users_good_partition'
AND active = 1;


-- Step 3
INSERT INTO users_good_partition VALUES
(30000000, 1000, '2025-01-15 10:00:00', 'Test User 1', '2025-01-15 10:00:00', '', 'Test City', '', 100, 50, 5, 5000000),
(30000001, 1500, '2025-02-15 10:00:00', 'Test User 2', '2025-02-15 10:00:00', '', 'Test City', '', 150, 75, 3, 5000001),
(30000002, 2000, '2025-03-15 10:00:00', 'Test User 3', '2025-03-15 10:00:00', '', 'Test City', '', 200, 100, 2, 5000002),
(30000003, 2500, '2025-04-15 10:00:00', 'Test User 4', '2025-04-15 10:00:00', '', 'Test City', '', 250, 125, 1, 5000003),
(30000004, 3000, '2025-05-15 10:00:00', 'Test User 5', '2025-05-15 10:00:00', '', 'Test City', '', 300, 150, 0, 5000004);


-- Step 4
SELECT 
count() AS total_parts,
uniqExact(partition) AS total_partitions,
sum(rows) AS total_rows,
formatReadableSize(sum(bytes_on_disk)) AS total_size
FROM system.parts
WHERE table = 'users_good_partition'
AND active = 1;

-- Step 5
SELECT 
location,
count() AS user_count,
avg(reputation) AS avg_reputation
FROM users_good_partition
WHERE toYYYYMM(creation_date) = 202102  -- February 2021
GROUP BY location
ORDER BY user_count DESC;


-- Step 6
SELECT 
display_name,
reputation,
creation_date,
location
FROM users_good_partition
WHERE user_id = 1200177;




