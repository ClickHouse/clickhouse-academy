-- Step 1: What are the 10 most common badges?
SELECT
    name,
    count(*) AS number_of_badges
FROM badges
GROUP BY name
ORDER BY number_of_badges DESC
LIMIT 10;

-- Step 2: Who are the 10 users with the most badges?
SELECT
    user_id,
    count()
FROM badges
GROUP BY user_id
ORDER BY 2 DESC
LIMIT 10;

-- Step 3: What is the average number of badges per user?
-- Note that using uniqExact is discouraged, unless really needed.
SELECT
    count()/uniq(user_id)
FROM
    badges;

-- OR

SELECT
    avg(badge_count) AS avg_badges_per_user
FROM (
    SELECT user_id, count(*) AS badge_count
    FROM badges
    GROUP BY user_id
);

-- Step 4: What are the minimum, maximum and average number of badges issued (include the min and max days)?
SELECT
    min(count),
    argMin(day, count),
    max(count),
    argMax(day, count),
    avg(count)
FROM
    (SELECT toDate(date) as day, count() as count FROM badges GROUP BY day);

-- Step 5: What was the badge distribution by day in 2021?
SELECT
    toDate(date) AS day,
    count() AS count
FROM badges
WHERE day >= toDate('2021-01-01') AND day < toDate('2022-01-01')
GROUP BY day;

-- Step 6: What is the badge distribution over time by month?
SELECT
    toStartOfMonth(date) AS month,
    count() AS count
FROM badges
GROUP BY month;

-- Step 7: At least how many badges do half of the users have? What about 25%, 20%, and 1% of the users?
SELECT
    quantiles(0.50, 0.75, 0.90, 0.99)(count)
FROM
    (SELECT count() as count FROM badges GROUP BY user_id);
