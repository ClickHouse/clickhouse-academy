-- Step 1
SELECT 
  toDate(date) AS award_date,
  COUNT(*) AS badges_awarded
FROM badges
GROUP BY award_date
ORDER BY award_date DESC;

-- Step 2
SELECT 
    name,
    COUNT(*) AS times_awarded,
    COUNT(DISTINCT user_id) AS unique_users
FROM badges
GROUP BY name
ORDER BY times_awarded DESC
LIMIT 5;


-- Step 3
SELECT 
    u.id,
    u.display_name,
    u.reputation
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet') AS u
WHERE u.id NOT IN (SELECT DISTINCT user_id FROM badges);

-- Step 4
SELECT
   PROJECT,
   count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_{0..5}.snappy.parquet')
GROUP BY PROJECT
ORDER BY 2 DESC;


-- Step 5
CREATE TABLE pypi
(
    `date` Date,
    `country_code` LowCardinality(String),
    `project` String,
    `type` LowCardinality(String),
    `installer` LowCardinality(String),
    `python_minor` LowCardinality(String),
    `system` LowCardinality(String),
    `version` String
)
ENGINE = MergeTree
ORDER BY (project, date, version);

-- Step 6
INSERT INTO pypi
SELECT
    toDate(timestamp) AS date,
    country_code,
    project,
    file.type AS type,
    installer.name AS installer,
    substring(python, 1, position(python, '.', position(python, '.') + 1) - 1) AS python_minor,
    implementation.name AS system,
    file.version AS version
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_{0..5}.snappy.parquet');


-- Step 7
SELECT
   PROJECT,
   count()
FROM pypi
GROUP BY PROJECT
ORDER BY 2 DESC;



