-- Step 1
SELECT
    name,
    count() AS total
FROM badges
GROUP BY name
ORDER BY total DESC
LIMIT 10;

-- Step 2
SELECT
    user_id,
    count() AS total
FROM badges
GROUP BY user_id
ORDER BY total DESC
LIMIT 10;

-- Step 3
SELECT
    avg(badge_count) AS avg_badges_per_user
FROM (
    SELECT user_id, count(*) AS badge_count
    FROM badges
    GROUP BY user_id
);

-- Step 4

-- Step 5
SELECT
    toDate(date) AS day,
    count() AS count
FROM badges
WHERE day >= toDate(now()) - INTERVAL 1 YEAR
GROUP BY day;

-- Step 6
SELECT
    toStartOfMonth(date) AS month,
    count() AS count
FROM badges
GROUP BY month;

-- Step 7
