-- Step 1
SELECT * FROM gold.dim_user_activity_marketing;

-- Step 2
SELECT 
  location,
  count() AS user_count
FROM gold.dim_user_activity_marketing
WHERE multiFuzzyMatchAny(location, 2, ['Bangalore', 'Bengaluru'])
GROUP BY location
ORDER BY user_count DESC;

-- There are a couple misspellings of Bangalore as 'Banglore'


SELECT count() AS total_users
FROM dim_user_activity_marketing
WHERE position(location, 'Bangalore') > 0
OR position(location, 'Bengaluru') > 0
OR position(location, 'Banglore') > 0;
-- 1072 users from Bengaluru

-- Step 3
SELECT
  quantiles(0.25, 0.50, 0.75, 0.90)(reputation) AS reputation_percentiles
FROM gold.dim_user_activity_mods;
-- [1,21,302.25,1808.2000000000044]

-- Step 4
-- Monthly Growth
SELECT 
  toStartOfMonth(activity_date) as month,
  sum(new_users) as new_users,
  sum(total_badges_awarded) as badges
FROM gold.fct_activity_by_time
WHERE activity_date >'2024-01-14' - INTERVAL 12 MONTH
GROUP BY month
ORDER BY month;


-- Step 5
SELECT
  corr(reputation, total_answers_posted) AS pearson_coeff,
  cramersV(reputation, total_answers_posted) AS cramers_v_coeff
FROM gold.dim_user_activity_mods;


-- Step 6
SELECT
  reputation,
  total_answers_posted
FROM gold.dim_user_activity_mods;


-- Step 7
SELECT simpleLinearRegression(reputation, total_answers_posted)
FROM gold.dim_user_activity_marketing


-- Step 8
--As reputation increases, total answers posted seems to increase as well. However, there is a large cluster of users who have low reputation, but have answered a lot of questions. 


-- Step 9
SELECT 
    d.reputation AS x,
    d.total_answers_posted AS y,
   0.78 * d.reputation - 61.2 AS predicted_answers
FROM gold.dim_user_activity_mods d
WHERE d.reputation < 10000 
  AND d.total_answers_posted < 500
ORDER BY d.reputation;

