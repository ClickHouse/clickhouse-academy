-- Step 2
SHOW CREATE TABLE badges FORMAT Vertical;

-- Step 3
SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM badges FORMAT Vertical;

-- Step 5
-- The user_id, name, and date columns are all great candidates
-- What are the 10 most common badges?
SELECT
    name,
    count(*) AS number_of_badges
FROM badges
GROUP BY name
ORDER BY number_of_badges DESC
LIMIT 10;

-- Who are the 10 users with the most badges?
SELECT
    user_id,
    count()
FROM badges
GROUP BY user_id
ORDER BY 2 DESC
LIMIT 10;


-- What was the badge distribution by day in January 2024?
SELECT
    toDate(date) AS day,
    count() AS count
FROM badges
WHERE day >= toDate('2021-01-01') AND day < toDate('2022-01-01')
GROUP BY day;


-- Step 6
-- The name column has the lowest cardinality, so it may as well be first, followed by user_id, then date (which is often at the end of a primary key)

-- Step 7
RENAME TABLE badges TO inferred_schema_badges;

CREATE TABLE badges (
    id UInt32,
    user_id Int32,
    name LowCardinality(String),
    date DateTime,
    class Enum('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    tag_based Bool
)
ENGINE = MergeTree
PRIMARY KEY (name, user_id, date);

-- Step 8
INSERT INTO badges
SELECT * FROM inferred_schema_badges;

-- Step 9
SELECT count() FROM badges;

-- Step 10
SELECT * FROM badges LIMIT 1000;

-- Step 11

-- posts

SHOW CREATE TABLE posts FORMAT Vertical;

SELECT 
    * APPLY min, 
    * APPLY  max, 
    * APPLY  uniq
FROM posts FORMAT Vertical;

RENAME TABLE posts TO inferred_schema_posts;

CREATE TABLE posts
(
    id                         UInt32,
    post_type_id               UInt8,
    accepted_answer_id         Nullable(UInt32),
    parent_id                  Nullable(UInt32),
    creation_date              DateTime,
    deletion_date              DateTime,
    score                      Int32,
    view_count                 UInt32,
    owner_user_id              Nullable(Int32),
    owner_display_name         String,
    last_editor_user_id        Nullable(Int32),
    last_editor_display_name   String,
    last_edit_date             DateTime,
    last_activity_date         DateTime,
    title                      String,
    tags                       String,
    answer_count               UInt16,
    comment_count              UInt16,
    favorite_count             UInt32,
    content_license            LowCardinality(String),
    closed_date                DateTime,
    community_owned_date       DateTime
)
ENGINE = MergeTree
PRIMARY KEY (post_type_id, owner_display_name, creation_date);

INSERT INTO posts SELECT * FROM inferred_schema_posts;

-- users

SHOW CREATE TABLE users FORMAT Vertical;

SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM users FORMAT Vertical;

RENAME TABLE users TO inferred_schema_users;

CREATE TABLE users
(
    id                Int32,
    reputation        UInt32,
    creation_date     DateTime,
    display_name      String,
    last_access_date  DateTime,
    website_url       String,
    location          String,
    about_me          String,
    views             UInt32,
    up_votes          UInt32,
    down_votes        UInt32,
    account_id        Nullable(Int32)
)
ENGINE = MergeTree
PRIMARY KEY (location, creation_date);

INSERT INTO users SELECT * FROM inferred_schema_users;

-- votes

SHOW CREATE TABLE votes FORMAT Vertical;

SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM votes FORMAT Vertical;

RENAME TABLE votes TO inferred_schema_votes;

CREATE TABLE votes
(
    id             UInt32,
    post_id        UInt32,
    vote_type_id   UInt8,
    user_id        Int32,
    creation_date  DateTime,
    bounty_amount  UInt32
)
ENGINE = MergeTree
PRIMARY KEY (user_id, post_id, creation_date);

INSERT INTO votes SELECT * FROM inferred_schema_votes;

-- post_types

SHOW CREATE TABLE post_types FORMAT Vertical;

SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM post_types FORMAT Vertical;

RENAME TABLE post_types TO inferred_schema_post_types;

CREATE TABLE post_types
(
    id    UInt8,
    name  LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (id, name);

INSERT INTO post_types SELECT * FROM inferred_schema_post_types;

-- vote_types

SHOW CREATE TABLE vote_types FORMAT Vertical;

SELECT
    * APPLY min,
    * APPLY  max,
    * APPLY  uniq
FROM vote_types FORMAT Vertical;

RENAME TABLE vote_types TO inferred_schema_vote_types;

CREATE TABLE vote_types
(
    id    UInt8,
    name  LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (id, name);

INSERT INTO vote_types SELECT * FROM inferred_schema_vote_types;
