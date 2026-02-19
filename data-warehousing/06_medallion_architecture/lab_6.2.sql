-- Step 1
CREATE OR REPLACE TABLE silver.int_badges
(
badge_id UInt64,
user_id Int64,
badge_name String,
badge_awarded_at DateTime,
badge_class UInt8,
class_name String,

tag_based UInt8,
processing_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (badge_id, user_id, badge_awarded_at)
PARTITION BY toYYYYMM(badge_awarded_at);

CREATE MATERIALIZED VIEW silver.int_badges_mv
TO silver.int_badges
AS
SELECT
badge_id,
user_id,
badge_name,
badge_awarded_at,
badge_class,
CASE
WHEN badge_class = 1 THEN 'Gold'
WHEN badge_class = 2 THEN 'Silver'
WHEN badge_class = 3 THEN 'Bronze'
ELSE 'Unknown'
END AS class_name,
tag_based,
now() AS processing_timestamp
FROM bronze.stg_badges;


-- Step 2
CREATE OR REPLACE TABLE silver.int_votes
(
  vote_id UInt64,
  post_id UInt64,
  vote_type_id UInt8,
  vote_type_name String,            
  user_id Nullable(UInt64),
  voted_at DateTime,
  bounty_amount Nullable(UInt16),
  is_deleted UInt8,
  processing_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (vote_id, post_id)
PARTITION BY toYYYYMM(voted_at);


CREATE MATERIALIZED VIEW silver.int_votes_mv
TO silver.int_votes
AS
SELECT 
  v.vote_id,
  v.post_id,
  v.vote_type_id,
  COALESCE(vt.vote_type_name, 'Unknown') AS vote_type_name,
  v.user_id,
  v.voted_at,
  v.bounty_amount,
  0 as is_deleted,
  now() AS processing_timestamp
FROM bronze.stg_votes v 
LEFT JOIN bronze.stg_vote_types vt ON v.vote_type_id = vt.vote_type_id
SETTINGS join_algorithm = 'direct';


-- Step 3
CREATE OR REPLACE TABLE silver.int_posts
(
  post_id UInt64,
  post_type_id UInt8,
  post_type_name String,           
  accepted_answer_id UInt64,
  parent_post_id UInt64,
  post_created_at DateTime,
  post_deleted_at DateTime,
  score Int32,
  view_count UInt32,
  owner_user_id UInt64,
  owner_display_name String,
  last_editor_user_id UInt64,
  last_editor_display_name String,
  last_edited_at DateTime,
  last_activity_at DateTime,
  title String,
  tags Array(String),              
  answer_count UInt16,
  comment_count UInt16,
  favorite_count UInt16,
  content_license String,
  closed_at DateTime,
  community_owned_at DateTime,
  is_question UInt8,               
  is_answer UInt8,                 
  is_deleted UInt8,                
  processing_timestamp DateTime DEFAULT now(),
)
ENGINE = MergeTree
ORDER BY (post_created_at, post_id)
PARTITION BY toYYYYMM(post_created_at);


CREATE MATERIALIZED VIEW silver.int_posts_mv
TO silver.int_posts
AS
SELECT 
  p.post_id,
  p.post_type_id,
  COALESCE(pt.post_type_name, 'Unknown') AS post_type_name,
  p.accepted_answer_id,
  p.parent_post_id,
  p.post_created_at,
  p.post_deleted_at,
  p.score,
  p.view_count,
  p.owner_user_id,
  p.owner_display_name,
  p.last_editor_user_id,
  p.last_editor_display_name,
  p.last_edited_at,
  p.last_activity_at,
  p.title,
  arrayFilter(x -> x != '', splitByChar('|', trim(BOTH '|' FROM p.tags))) AS tags,
  p.answer_count,
  p.comment_count,
  p.favorite_count,
  p.content_license,
  p.closed_at,
  p.community_owned_at,
  if(p.post_type_id = 1, 1, 0) AS is_question,
  if(p.post_type_id = 2, 1, 0) AS is_answer,
  if(p.post_deleted_at > toDateTime('1970-01-01 00:00:01'), 1, 0) AS is_deleted,
  now() AS processing_timestamp
FROM bronze.stg_posts AS p
LEFT JOIN bronze.stg_post_types pt ON p.post_type_id = pt.post_type_id
SETTINGS join_algorithm = 'direct';


-- Step 4
CREATE OR REPLACE TABLE silver.int_users
(
  user_id Int64,
  reputation UInt32,
  created_at DateTime,
  display_name String,
  last_access_at DateTime,
  website_url String,
  location String,
  about_me String,
  profile_views UInt32,
  up_votes UInt32,
  down_votes UInt32,
  account_id Int64,
  days_since_last_access Int32,   
  processing_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (user_id, created_at)
PARTITION BY toYYYYMM(created_at);



CREATE MATERIALIZED VIEW silver.int_users_mv
TO silver.int_users
AS
SELECT 
  user_id,
  reputation,
  created_at,
  display_name,
  last_access_at,
  website_url,
  location,
  about_me,
  profile_views,
  up_votes,
  down_votes,
  account_id,
  dateDiff('day', last_access_at, now()) AS days_since_last_access,
  now() AS processing_timestamp
FROM bronze.stg_users 
WHERE user_id > 0;  -- Filter out invalid users (Community user is -1)


-- Step 5
INSERT INTO silver.int_badges
SELECT
    badge_id,
    user_id,
    badge_name,
    badge_awarded_at,
    badge_class,
    CASE
        WHEN badge_class = 1 THEN 'Gold'
        WHEN badge_class = 2 THEN 'Silver'
        WHEN badge_class = 3 THEN 'Bronze'
        ELSE 'Unknown'
    END AS class_name,
    tag_based,
    now() AS processing_timestamp
FROM bronze.stg_badges;

INSERT INTO silver.int_votes
SELECT 
    v.vote_id,
    v.post_id,
    v.vote_type_id,
    COALESCE(vt.vote_type_name, 'Unknown') AS vote_type_name,
    v.user_id,
    v.voted_at,
    v.bounty_amount,
    0 AS is_deleted,
    now() AS processing_timestamp
FROM bronze.stg_votes v 
LEFT JOIN bronze.stg_vote_types vt ON v.vote_type_id = vt.vote_type_id
SETTINGS join_algorithm = 'direct';


INSERT INTO silver.int_posts
SELECT 
    p.post_id,
    p.post_type_id,
    COALESCE(pt.post_type_name, 'Unknown') AS post_type_name,
    p.accepted_answer_id,
    p.parent_post_id,
    p.post_created_at,
    p.post_deleted_at,
    p.score,
    p.view_count,
    p.owner_user_id,
    p.owner_display_name,
    p.last_editor_user_id,
    p.last_editor_display_name,
    p.last_edited_at,
    p.last_activity_at,
    p.title,
    arrayFilter(x -> x != '', splitByChar('|', trim(BOTH '|' FROM p.tags))) AS tags,
    p.answer_count,
    p.comment_count,
    p.favorite_count,
    p.content_license,
    p.closed_at,
    p.community_owned_at,
    if(p.post_type_id = 1, 1, 0) AS is_question,
    if(p.post_type_id = 2, 1, 0) AS is_answer,
    if(p.post_deleted_at > toDateTime('1970-01-01 00:00:01'), 1, 0) AS is_deleted,
    now() AS processing_timestamp
FROM bronze.stg_posts AS p
LEFT JOIN bronze.stg_post_types pt ON p.post_type_id = pt.post_type_id
SETTINGS join_algorithm = 'direct';


INSERT INTO silver.int_users
SELECT 
    user_id,
    reputation,
    created_at,
    display_name,
    last_access_at,
    website_url,
    location,
    about_me,
    profile_views,
    up_votes,
    down_votes,
    account_id,
    dateDiff('day', last_access_at, now()) AS days_since_last_access,
    now() AS processing_timestamp
FROM bronze.stg_users 
WHERE user_id > 0;  -- Filter out invalid users (Community user is -1)
