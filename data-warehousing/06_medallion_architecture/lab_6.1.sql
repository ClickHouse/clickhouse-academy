-- Step 1
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Step 2
CREATE OR REPLACE TABLE bronze.stg_post_types
(
  post_type_id UInt16,
  post_type_name LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY post_type_id;


INSERT INTO bronze.stg_post_types
SELECT
  id AS post_type_id,
  name AS post_type_name
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/post_types.csv', 'CSVWithNames');

CREATE OR REPLACE TABLE bronze.stg_vote_types
(
  vote_type_id UInt16,
  vote_type_name LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY vote_type_id;


INSERT INTO bronze.stg_vote_types
SELECT
  id AS vote_type_id,
  name AS vote_type_name
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/vote_types.csv', 'CSVWithNames');


-- Step 3
CREATE OR REPLACE TABLE bronze.stg_badges
(
  badge_id UInt64,
  user_id Int64,
  badge_name String,
  badge_awarded_at DateTime,
  badge_class UInt8,
  tag_based Bool,
  load_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (badge_awarded_at, badge_name, user_id)
PARTITION BY toYYYYMM(badge_awarded_at);

INSERT INTO bronze.stg_badges
SELECT
  id AS badge_id,
  user_id,
  name AS badge_name,
  date AS badge_awarded_at,
  class AS badge_class,
  tag_based,
  now() as load_timestamp
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/badges.parquet', 'Parquet');


-- Step 4
CREATE OR REPLACE TABLE bronze.stg_posts
(
  post_id UInt64,
  post_type_id UInt8,
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
  tags String,
  answer_count UInt16,             
  comment_count UInt16,           
  favorite_count UInt16,           
  content_license String,
  closed_at DateTime,               
  community_owned_at DateTime,
  load_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (post_id)
PARTITION BY toYYYYMM(post_created_at);

INSERT INTO bronze.stg_posts
SELECT
  id as post_id,
  post_type_id,
  accepted_answer_id,
  parent_id as parent_post_id,
  creation_date as post_created_at,
  deletion_date as post_deleted_at,              
  score,                      
  view_count,               
  owner_user_id,
  owner_display_name,
  last_editor_user_id,
  last_editor_display_name,
  last_edit_date as last_edited_at,          
  last_activity_date as last_activity_at,        
  title,
  tags,
  answer_count,             
  comment_count,           
  favorite_count,           
  content_license,
  closed_date as closed_at,               
  community_owned_date as community_owned_at,
  now() as load_timestamp
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/posts.parquet', 'Parquet');


-- Step 5
CREATE OR REPLACE TABLE bronze.stg_votes
(
  vote_id UInt64,
  post_id UInt64,
  vote_type_id UInt8,
  user_id Nullable(UInt64),
  voted_at DateTime,
  bounty_amount Nullable(UInt16),
  load_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (post_id, vote_id)
PARTITION BY toYYYYMM(voted_at);


INSERT INTO bronze.stg_votes
SELECT 
  id AS vote_id,
  post_id,
  vote_type as vote_type_id,
  user_id,
  creation_date AS voted_at,
  bounty_amount,
  now() AS load_timestamp
FROM icebergS3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/iceberg-tables/votes/', NOSIGN);


-- Step 6
CREATE OR REPLACE TABLE bronze.stg_users
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
  load_timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (user_id, load_timestamp)
PARTITION BY toYYYYMM(load_timestamp);


INSERT INTO bronze.stg_users
SELECT 
  id AS user_id,
  reputation,
  creation_date AS created_at,
  display_name,
  last_access_date AS last_access_at,
  website_url,
  location,
  about_me,
  views AS profile_views,
  up_votes,
  down_votes,
  account_id,
  now() AS load_timestamp
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet');




