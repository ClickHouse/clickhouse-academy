-- Step 5
SELECT table_name, ddl 
FROM bigquery-public-data.stackoverflow.INFORMATION_SCHEMA.TABLES;
 
-- Step 6

SELECT *
FROM bigquery-public-data.stackoverflow.comments limit 100;

-- Step 7

SELECT
    user_id,
    count(*) AS count
FROM bigquery-public-data.stackoverflow.badges
GROUP BY user_id
ORDER BY count DESC
LIMIT 10;

-- Step 8

SELECT
    avg(badge_count) AS avg_badges_per_user
FROM (
    SELECT user_id, count(*) AS badge_count
    FROM bigquery-public-data.stackoverflow.badges
    GROUP BY user_id
);

-- Step 9

SELECT
    TIMESTAMP_TRUNC(date, month, 'UTC') AS start_of_month,
    count(*) AS count
FROM bigquery-public-data.stackoverflow.badges
GROUP BY start_of_month;

-- Step 10

SELECT 
  q.owner_user_id
FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
WHERE
      EXTRACT (MONTH FROM q.creation_date) = 7
AND EXTRACT (YEAR FROM q.creation_date) = 2020
UNION DISTINCT
SELECT 
  a.owner_user_id
FROM `bigquery-public-data.stackoverflow.posts_answers` AS a
WHERE
      EXTRACT (MONTH FROM a.creation_date) = 7
AND EXTRACT (YEAR FROM a.creation_date) = 2020;
