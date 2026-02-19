-- Step 1
SELECT * FROM badges;

SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/posts.parquet', 'Parquet');

SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet');

SELECT * FROM icebergS3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/iceberg-tables/votes/', NOSIGN);


SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/post_types.csv');

SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/vote_types.csv');


DESCRIBE badges;
DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/posts.parquet', 'Parquet');

DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet');

DESCRIBE icebergS3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/iceberg-tables/votes/', NOSIGN);

DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/post_types.csv');

DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/vote_types.csv');


-- Step 2
CREATE OR REPLACE TABLE posts (
    id Int32,
    post_type_id UInt8,
    accepted_answer_id Nullable(Int32),
    parent_id Nullable(Int32),
    creation_date DateTime,
    deletion_date DateTime,
    score Int32,
    view_count Nullable(Int32),
    owner_user_id Nullable(Int32),
    owner_display_name String,
    last_editor_user_id Nullable(Int32),
    last_editor_display_name String,
    last_edit_date DateTime,
    last_activity_date DateTime,
    title String,
    tags String,
    answer_count Nullable(Int32),
    comment_count Int32,
    favorite_count Nullable(Int32),
    content_license String,
    community_owned_date DateTime,
    closed_date DateTime
)
ENGINE = MergeTree
PRIMARY KEY (creation_date, id);

CREATE OR REPLACE TABLE users(
    id Int32,
    reputation Int32,
    creation_date DateTime,
    display_name String,
    last_access_date DateTime,
    website_url String,
    location String,
    about_me String,
    views Int64,
    up_votes Int32,
    down_votes Int32,
    account_id String
)
ENGINE = MergeTree
PRIMARY KEY id;


CREATE OR REPLACE TABLE votes (
    id UInt32,
    post_id UInt32,
    vote_type LowCardinality(String),
    user_id Int32,
    creation_date DateTime,
    bounty_amount UInt16
)
ENGINE = MergeTree
PRIMARY KEY (vote_type, post_id, creation_date);

CREATE  OR REPLACE TABLE post_types (
  id Int16,
  name String
)
ENGINE = MergeTree
PRIMARY KEY id;

CREATE  OR REPLACE TABLE vote_types (
  id Int16,
  name String
)
ENGINE = MergeTree
PRIMARY KEY name;


-- Step 3
INSERT INTO posts
SELECT 
    id,
    post_type_id,
    accepted_answer_id ,
    parent_id,
    creation_date,
    deletion_date,
    score,
    view_count,
    owner_user_id,
    owner_display_name,
    last_editor_user_id ,
    last_editor_display_name,
    last_edit_date,
    last_activity_date,
    title,
    tags,
    answer_count,
    comment_count,
    favorite_count,
    content_license,
    community_owned_date,
    closed_date
	
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/posts.parquet', 'Parquet');

INSERT INTO users
SELECT 
    id,
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
s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/users.parquet', 'Parquet');

INSERT INTO votes
SELECT 
    id,
    post_id,
    vote_type_id as vote_type,
    user_id,
    creation_date,
    bounty_amount
FROM icebergS3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/iceberg-tables/votes/', NOSIGN);



INSERT INTO post_types
SELECT 
    id,
    name
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/post_types.csv');


INSERT INTO vote_types
SELECT 
    id,
    name
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/stack-exchange/vote_types.csv');;


-- Step 4
INSERT INTO posts (
    id,
    post_type_id,
    accepted_answer_id,
    parent_id,
    creation_date,
    deletion_date,
    score,
    view_count,
    owner_user_id,
    owner_display_name,
    last_editor_user_id,
    last_editor_display_name,
    last_edit_date,
    last_activity_date,
    title,
    tags,
    answer_count,
    comment_count,
    favorite_count,
    content_license,
    community_owned_date,
    closed_date
) VALUES (
    99999999,
    1,
    NULL,
    NULL,
    now(),
    '1970-01-01 00:00:00',
    5,
    150,
    12345678,
    'FakeUser123',
    12345678,
    'FakeUser123',
    now(),
    now(),
    'How to optimize ClickHouse queries for better performance?',
    '|clickhouse|performance|optimization|',
    0,
    0,
    NULL,
    'CC BY-SA 4.0',
    '1970-01-01 00:00:00',
    '1970-01-01 00:00:00'
);


-- only returns latest data
SELECT *
FROM posts
WHERE creation_date = (SELECT max(creation_date) FROM posts);
