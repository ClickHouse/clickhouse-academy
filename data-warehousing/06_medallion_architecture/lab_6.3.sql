-- Step 1
CREATE OR REPLACE TABLE gold.fct_activity_by_time
(
    activity_date Date,
    total_questions UInt32,
    total_answers UInt32,
    total_votes UInt32,
    total_badges_awarded UInt32,
    active_users UInt32,
    new_users UInt32,
    avg_question_score Float32,
    avg_answer_score Float32,
    questions_with_accepted_answers UInt32,
    avg_time_to_answer_minutes Float32,
    deleted_posts UInt32,
    total_bounties_awarded UInt32,
    bounty_amount_total UInt32,
    avg_views_per_question Float32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY activity_date;



CREATE MATERIALIZED VIEW gold.fct_activity_by_time_mv
REFRESH EVERY 1 HOUR
TO gold.fct_activity_by_time
AS
WITH

post_metrics AS (
    SELECT
        toDate(post_created_at) AS activity_date,
        countIf(is_question = 1 AND is_deleted = 0) AS total_questions,
        countIf(is_answer = 1 AND is_deleted = 0) AS total_answers,
        countIf(is_deleted = 1) AS deleted_posts,
        avgIf(score, is_question = 1 AND is_deleted = 0) AS avg_question_score,
        avgIf(score, is_answer = 1 AND is_deleted = 0) AS avg_answer_score,
        countIf(is_question = 1 AND accepted_answer_id > 0) AS questions_with_accepted_answers,
        avgIf(view_count, is_question = 1 AND is_deleted = 0) AS avg_views_per_question,
        uniq(owner_user_id) AS active_users
    FROM silver.int_posts
    GROUP BY activity_date
),


vote_metrics AS (
    SELECT
        toDate(voted_at) AS activity_date,
        count() AS total_votes,
        countIf(bounty_amount > 0) AS total_bounties_awarded,
        sumIf(bounty_amount, bounty_amount > 0) AS bounty_amount_total
    FROM silver.int_votes
    GROUP BY activity_date
),


badge_metrics AS (
    SELECT
        toDate(badge_awarded_at) AS activity_date,
        count() AS total_badges_awarded
    FROM silver.int_badges
    GROUP BY activity_date
),


user_metrics AS (
    SELECT
        toDate(created_at) AS activity_date,
        count() AS new_users
    FROM silver.int_users
    GROUP BY activity_date
)

-- Join all metrics together 
SELECT
    p.activity_date AS activity_date,
    p.total_questions AS total_questions,
    p.total_answers AS total_answers,
    COALESCE(v.total_votes, 0) AS total_votes,
    COALESCE(b.total_badges_awarded, 0) AS total_badges_awarded,
    p.active_users AS active_users,
    COALESCE(u.new_users, 0) AS new_users,
    p.avg_question_score AS avg_question_score,
    p.avg_answer_score AS avg_answer_score,
    p.questions_with_accepted_answers AS questions_with_accepted_answers,
    0 AS avg_time_to_answer_minutes,
    p.deleted_posts AS deleted_posts,
    COALESCE(v.total_bounties_awarded, 0) AS total_bounties_awarded,
    COALESCE(v.bounty_amount_total, 0) AS bounty_amount_total,
    p.avg_views_per_question AS avg_views_per_question,
    now() AS updated_at
FROM post_metrics p
LEFT JOIN vote_metrics v ON p.activity_date = v.activity_date
LEFT JOIN badge_metrics b ON p.activity_date = b.activity_date
LEFT JOIN user_metrics u ON p.activity_date = u.activity_date;

-- Step 2
-- marketing team user_activity table
CREATE TABLE gold.dim_user_activity_marketing (
  user_id Int64,
  display_name String,
  reputation UInt32,
  user_created_at DateTime,
  last_access_at DateTime,
  days_since_last_access Int32,
  location String,
  website_url String,
  profile_views UInt32,
  up_votes_given UInt32,
  down_votes_given UInt32,
  total_questions_asked UInt64,
  total_answers_posted UInt64,
  total_posts UInt64,
  avg_question_score Float64,
  avg_answer_score Float64,
  total_accepted_answers UInt64,
  total_post_views UInt64,
  total_votes_received UInt64,
  upvotes_received UInt64,
  downvotes_received UInt64,
  total_badges UInt64,
  gold_badges UInt64,
  silver_badges UInt64,
  bronze_badges UInt64,
  is_active_user UInt8,
  is_power_user UInt8,
  last_refreshed DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_created_at, is_active_user, is_power_user, user_id)  
PARTITION BY toYYYYMM(user_created_at);

-- content moderation team user activity table
CREATE TABLE gold.dim_user_activity_mods (
  user_id Int64,
  display_name String,
  reputation UInt32,
  user_created_at DateTime,
  last_access_at DateTime,
  days_since_last_access Int32,
  location String,
  website_url String,
  profile_views UInt32,
  up_votes_given UInt32,
  down_votes_given UInt32,
  total_questions_asked UInt64,
  total_answers_posted UInt64,
  total_posts UInt64,
  avg_question_score Float64,
  avg_answer_score Float64,
  total_accepted_answers UInt64,
  total_post_views UInt64,
  total_votes_received UInt64,
  upvotes_received UInt64,
  downvotes_received UInt64,
  total_badges UInt64,
  gold_badges UInt64,
  silver_badges UInt64,
  bronze_badges UInt64,
  is_active_user UInt8,
  is_power_user UInt8,
  last_refreshed DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (reputation, downvotes_received, user_id)  
PARTITION BY toYYYYMM(user_created_at);

-- customer success user activity table
CREATE TABLE gold.dim_user_activity_customer_success (
  user_id Int64,
  display_name String,
  reputation UInt32,
  user_created_at DateTime,
  last_access_at DateTime,
  days_since_last_access Int32,
  location String,
  website_url String,
  profile_views UInt32,
  up_votes_given UInt32,
  down_votes_given UInt32,
  total_questions_asked UInt64,
  total_answers_posted UInt64,
  total_posts UInt64,
  avg_question_score Float64,
  avg_answer_score Float64,
  total_accepted_answers UInt64,
  total_post_views UInt64,
  total_votes_received UInt64,
  upvotes_received UInt64,
  downvotes_received UInt64,
  total_badges UInt64,
  gold_badges UInt64,
  silver_badges UInt64,
  bronze_badges UInt64,
  is_active_user UInt8,
  is_power_user UInt8,
  last_refreshed DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id)  
PARTITION BY toYYYYMM(user_created_at);

CREATE MATERIALIZED VIEW gold.dim_user_activity_marketing_mv
REFRESH EVERY 1 HOUR
TO gold.dim_user_activity_marketing
AS
SELECT
  u.user_id AS user_id,
  u.display_name AS display_name,
  u.reputation AS reputation,
  u.created_at AS user_created_at,
  u.last_access_at AS last_access_at,
  u.days_since_last_access AS days_since_last_access,
  u.location AS location,
  u.website_url AS website_url,
  u.profile_views AS profile_views,
  u.up_votes AS up_votes_given,
  u.down_votes AS down_votes_given,
  
  -- Post metrics
  countIf(p.post_id IS NOT NULL AND p.is_question = 1 AND p.is_deleted = 0) AS total_questions_asked,
  countIf(p.post_id IS NOT NULL AND p.is_answer = 1 AND p.is_deleted = 0) AS total_answers_posted,
  count(p.post_id) AS total_posts,
  avgIf(p.score, p.is_question = 1) AS avg_question_score,
  avgIf(p.score, p.is_answer = 1) AS avg_answer_score,
  countIf(p.post_id IN (
      SELECT DISTINCT accepted_answer_id 
      FROM silver.int_posts 
      WHERE accepted_answer_id > 0
  )) AS total_accepted_answers,
  sumIf(p.view_count, p.is_question = 1) AS total_post_views,
  
  -- Voting metrics (JOIN votes through posts)
  count(v.vote_id) AS total_votes_received,
  countIf(v.vote_type_name = 'UpMod') AS upvotes_received,
  countIf(v.vote_type_name = 'DownMod') AS downvotes_received,
  
  -- Badge metrics
  count(b.badge_id) AS total_badges,
  countIf(b.badge_class = 1) AS gold_badges,
  countIf(b.badge_class = 2) AS silver_badges,
  countIf(b.badge_class = 3) AS bronze_badges,
  
  -- Activity flags
  if(u.days_since_last_access <= 30, 1, 0) AS is_active_user,
  if(u.reputation >= 10000, 1, 0) AS is_power_user,
  
  now() AS last_refreshed
FROM silver.int_users u
LEFT JOIN silver.int_posts AS p ON u.user_id = toInt64(p.owner_user_id)
LEFT JOIN silver.int_votes v ON p.post_id = v.post_id
LEFT JOIN silver.int_badges b ON u.user_id = b.user_id
GROUP BY 
  user_id, display_name, reputation, user_created_at, 
  last_access_at, days_since_last_access, location, 
  website_url, profile_views, up_votes_given, down_votes_given;


-- create content mod mv

CREATE MATERIALIZED VIEW gold.dim_user_activity_mods_mv
REFRESH EVERY 1 HOUR
TO gold.dim_user_activity_mods
AS
SELECT
  u.user_id AS user_id,
  u.display_name AS display_name,
  u.reputation AS reputation,
  u.created_at AS user_created_at,
  u.last_access_at AS last_access_at,
  u.days_since_last_access AS days_since_last_access,
  u.location AS location,
  u.website_url AS website_url,
  u.profile_views AS profile_views,
  u.up_votes AS up_votes_given,
  u.down_votes AS down_votes_given,
  
  -- Post metrics
  countIf(p.post_id IS NOT NULL AND p.is_question = 1 AND p.is_deleted = 0) AS total_questions_asked,
  countIf(p.post_id IS NOT NULL AND p.is_answer = 1 AND p.is_deleted = 0) AS total_answers_posted,
  count(p.post_id) AS total_posts,
  avgIf(p.score, p.is_question = 1) AS avg_question_score,
  avgIf(p.score, p.is_answer = 1) AS avg_answer_score,
  countIf(p.post_id IN (
      SELECT DISTINCT accepted_answer_id 
      FROM silver.int_posts 
      WHERE accepted_answer_id > 0
  )) AS total_accepted_answers,
  sumIf(p.view_count, p.is_question = 1) AS total_post_views,
  
  -- Voting metrics (JOIN votes through posts)
  count(v.vote_id) AS total_votes_received,
  countIf(v.vote_type_name = 'UpMod') AS upvotes_received,
  countIf(v.vote_type_name = 'DownMod') AS downvotes_received,
  
  -- Badge metrics
  count(b.badge_id) AS total_badges,
  countIf(b.badge_class = 1) AS gold_badges,
  countIf(b.badge_class = 2) AS silver_badges,
  countIf(b.badge_class = 3) AS bronze_badges,
  
  -- Activity flags
  if(u.days_since_last_access <= 30, 1, 0) AS is_active_user,
  if(u.reputation >= 10000, 1, 0) AS is_power_user,
  
  now() AS last_refreshed
FROM silver.int_users u
LEFT JOIN silver.int_posts AS p ON u.user_id = toInt64(p.owner_user_id)
LEFT JOIN silver.int_votes v ON p.post_id = v.post_id
LEFT JOIN silver.int_badges b ON u.user_id = b.user_id
GROUP BY 
  user_id, display_name, reputation, user_created_at, 
  last_access_at, days_since_last_access, location, 
  website_url, profile_views, up_votes_given, down_votes_given;


-- create customer success mv

CREATE MATERIALIZED VIEW gold.dim_user_activity_customer_success_mv
REFRESH EVERY 1 HOUR
TO gold.dim_user_activity_customer_success
AS
SELECT
  u.user_id AS user_id,
  u.display_name AS display_name,
  u.reputation AS reputation,
  u.created_at AS user_created_at,
  u.last_access_at AS last_access_at,
  u.days_since_last_access AS days_since_last_access,
  u.location AS location,
  u.website_url AS website_url,
  u.profile_views AS profile_views,
  u.up_votes AS up_votes_given,
  u.down_votes AS down_votes_given,
  
  -- Post metrics
  countIf(p.post_id IS NOT NULL AND p.is_question = 1 AND p.is_deleted = 0) AS total_questions_asked,
  countIf(p.post_id IS NOT NULL AND p.is_answer = 1 AND p.is_deleted = 0) AS total_answers_posted,
  count(p.post_id) AS total_posts,
  avgIf(p.score, p.is_question = 1) AS avg_question_score,
  avgIf(p.score, p.is_answer = 1) AS avg_answer_score,
  countIf(p.post_id IN (
      SELECT DISTINCT accepted_answer_id 
      FROM silver.int_posts 
      WHERE accepted_answer_id > 0
  )) AS total_accepted_answers,
  sumIf(p.view_count, p.is_question = 1) AS total_post_views,
  
  -- Voting metrics (JOIN votes through posts)
  count(v.vote_id) AS total_votes_received,
  countIf(v.vote_type_name = 'UpMod') AS upvotes_received,
  countIf(v.vote_type_name = 'DownMod') AS downvotes_received,
  
  -- Badge metrics
  count(b.badge_id) AS total_badges,
  countIf(b.badge_class = 1) AS gold_badges,
  countIf(b.badge_class = 2) AS silver_badges,
  countIf(b.badge_class = 3) AS bronze_badges,
  
  -- Activity flags
  if(u.days_since_last_access <= 30, 1, 0) AS is_active_user,
  if(u.reputation >= 10000, 1, 0) AS is_power_user,
  
  now() AS last_refreshed
FROM silver.int_users u
LEFT JOIN silver.int_posts AS p ON u.user_id = toInt64(p.owner_user_id)
LEFT JOIN silver.int_votes v ON p.post_id = v.post_id
LEFT JOIN silver.int_badges b ON u.user_id = b.user_id
GROUP BY 
  user_id, display_name, reputation, user_created_at, 
  last_access_at, days_since_last_access, location, 
  website_url, profile_views, up_votes_given, down_votes_given;


-- Step 3
CREATE OR REPLACE TABLE gold.dim_tag_performance
(
tag_name String,

  -- Volume metrics
total_questions UInt32,
total_views UInt64,
total_answers UInt32,

  -- Engagement metrics
avg_score_per_question Float32,
avg_answers_per_question Float32,
avg_views_per_question Float32,
questions_with_accepted_answers UInt32,
acceptance_rate Float32,

  -- User metrics
unique_askers UInt32,
unique_answerers UInt32,

  -- Activity metrics
questions_last_7_days UInt32,
questions_last_30_days UInt32,

  -- Classification
is_trending UInt8,

updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (total_questions, tag_name);


CREATE MATERIALIZED VIEW gold.dim_tag_performance_mv
REFRESH EVERY 4 HOUR
TO gold.dim_tag_performance
AS
SELECT
arrayJoin(tags) AS tag_name,

  -- Volume metrics
count() AS total_questions,
sum(view_count) AS total_views,
sum(answer_count) AS total_answers,

  -- Engagement metrics
avg(score) AS avg_score_per_question,
avg(answer_count) AS avg_answers_per_question,
avg(view_count) AS avg_views_per_question,
countIf(accepted_answer_id > 0) AS questions_with_accepted_answers,
countIf(accepted_answer_id > 0) / count() AS acceptance_rate,

  -- User metrics
uniq(owner_user_id) AS unique_askers,
0 AS unique_answerers, -- Would need to join with answers

  -- Activity metrics
countIf(post_created_at >= now() - INTERVAL 7 DAY) AS questions_last_7_days,
countIf(post_created_at >= now() - INTERVAL 30 DAY) AS questions_last_30_days,

if(countIf(post_created_at >= now() - INTERVAL 7 DAY) > avg(count()) OVER (), 1, 0) AS is_trending,

now() AS updated_at
FROM silver.int_posts
WHERE is_question = 1 AND is_deleted = 0 AND length(tags) > 0
GROUP BY tag_name
HAVING count() >= 10;


-- Step 4
CREATE OR REPLACE TABLE gold.fact_post_performance
(
    post_id UInt64,
    post_type String,
    is_question UInt8,
    is_answer UInt8,
    
    -- Post metadata
    created_at DateTime,
    owner_user_id UInt64,
    owner_display_name String,
    owner_reputation UInt32,
    title String,
    tags Array(String),
    
    -- Performance metrics
    score Int32,
    view_count UInt32,
    answer_count UInt16,
    comment_count UInt16,
    favorite_count UInt16,
    
    -- Engagement metrics
    total_votes_received UInt32,
    upvotes UInt32,
    downvotes UInt32,
    
    -- Quality indicators
    has_accepted_answer UInt8,
    time_to_accepted_answer_hours Float32,
    is_closed UInt8,
    is_deleted UInt8,
    days_since_last_activity UInt32,
    
    -- Performance classification
    performance_tier String,
    
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (created_at, performance_tier, post_id);


CREATE MATERIALIZED VIEW gold.fact_post_performance_mv
REFRESH EVERY 1 HOUR
TO gold.fact_post_performance
AS
WITH vote_aggregates AS (
    SELECT 
        post_id,
        count() AS total_votes_received,
        countIf(vote_type_id = 2) AS upvotes,
        countIf(vote_type_id = 3) AS downvotes
    FROM silver.int_votes
    GROUP BY post_id
)
SELECT
    p.post_id AS post_id,
    p.post_type_name AS post_type,
    p.is_question AS is_question,
    p.is_answer AS is_answer,
    
    -- Metadata
    p.post_created_at AS created_at,
    p.owner_user_id AS owner_user_id,
    p.owner_display_name AS owner_display_name,
    COALESCE(u.reputation, 0) AS owner_reputation,
    p.title AS title,
    p.tags AS tags,
    
    -- Performance metrics
    p.score AS score,
    p.view_count AS view_count,
    p.answer_count AS answer_count,
    p.comment_count AS comment_count,
    p.favorite_count AS favorite_count,
    
    -- Vote breakdown (from JOIN)
    COALESCE(v.total_votes_received, 0) AS total_votes_received,
    COALESCE(v.upvotes, 0) AS upvotes,
    COALESCE(v.downvotes, 0) AS downvotes,
    
    -- Quality indicators
    if(p.accepted_answer_id > 0, 1, 0) AS has_accepted_answer,
    0 AS time_to_accepted_answer_hours,
    if(p.closed_at > toDateTime('1970-01-01'), 1, 0) AS is_closed,
    p.is_deleted AS is_deleted,
    dateDiff('day', p.last_activity_at, now()) AS days_since_last_activity,
    
    -- Performance classification
    CASE
        WHEN p.score >= 10 AND p.view_count >= 1000 THEN 'High'
        WHEN p.score >= 5 AND p.view_count >= 500 THEN 'Medium'
        WHEN p.score >= 0 THEN 'Low'
        ELSE 'Negative'
    END AS performance_tier,
    
    now() AS updated_at
FROM silver.int_posts p
LEFT JOIN silver.int_users u ON toInt64(p.owner_user_id) = u.user_id
LEFT JOIN vote_aggregates v ON p.post_id = v.post_id
WHERE p.is_deleted = 0;


-- Step 5
-- Target table for badge award analytics
CREATE OR REPLACE TABLE gold.fact_badge_awards
(
    badge_name String,
    badge_class String,
    badge_class_id UInt8,
    
    -- Volume metrics
    total_awards UInt32,
    unique_recipients UInt32,
    
    -- Timing metrics
    awards_last_7_days UInt32,
    awards_last_30_days UInt32,
    avg_days_to_earn Float32,
    
    -- Recipient profile
    avg_recipient_reputation Float32,
    is_tag_based UInt8,
    
    -- Activity indicators
    award_velocity Float32,
    is_rare UInt8,
    
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (badge_class, total_awards, badge_name);

-- Refreshable materialized view with JOIN instead of correlated subqueries
CREATE MATERIALIZED VIEW gold.fact_badge_awards_mv
REFRESH EVERY 6 HOUR
TO gold.fact_badge_awards
AS
SELECT
    b.badge_name AS badge_name,
    b.class_name AS badge_class,
    b.badge_class AS badge_class_id,
    
    -- Volume metrics
    count() AS total_awards,
    uniq(b.user_id) AS unique_recipients,
    
    -- Timing metrics
    countIf(b.badge_awarded_at >= now() - INTERVAL 7 DAY) AS awards_last_7_days,
    countIf(b.badge_awarded_at >= now() - INTERVAL 30 DAY) AS awards_last_30_days,
    avg(dateDiff('day', u.created_at, b.badge_awarded_at)) AS avg_days_to_earn,
    
    -- Recipient profile
    avg(u.reputation) AS avg_recipient_reputation,
    any(b.tag_based) AS is_tag_based,
    
    -- Activity indicators
    count() / greatest(dateDiff('day', min(b.badge_awarded_at), now()), 1) AS award_velocity,
    if(count() < 100, 1, 0) AS is_rare,
    
    now() AS updated_at
FROM silver.int_badges b
LEFT JOIN silver.int_users u ON b.user_id = u.user_id
GROUP BY b.badge_name, b.class_name, b.badge_class;


