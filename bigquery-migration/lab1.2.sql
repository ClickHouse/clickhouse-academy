-- Step 3

EXPORT DATA
  OPTIONS (
	uri = 'gs://my_bucket/stackoverflow/badges/badges.parquet',
	format = 'Parquet',
	overwrite = true
)
AS (
  SELECT *
  FROM bigquery-public-data.stackoverflow.badges
);

-- Step 9

SELECT count()
FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet');

-- Step 10

SELECT
    avg(badge_count) AS avg_badges_per_user
FROM (
    SELECT user_id, count(*) AS badge_count
    FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet')
    GROUP BY user_id
);

-- Step 11

SELECT
    toStartOfMonth(Date) AS start_of_month,
    count() AS count
FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/badges/badges.parquet')
GROUP BY start_of_month;
