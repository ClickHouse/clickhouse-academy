-- Step 1
CREATE DICTIONARY post_types (
    id UInt8,
    name String
)
PRIMARY KEY id
SOURCE(PostgreSQL(
    host '3.111.115.15'
    port 5432
    db 'stackexchange'
    table 'post_types'
    user 'stack_readonly_user'
    password 'clickhouse'
))
LAYOUT(FLAT)
LIFETIME(3600);

-- Step 2
SELECT * FROM post_types LIMIT 10;

-- Step 3
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
PRIMARY KEY id;

-- Step 4
INSERT INTO posts
SELECT * FROM postgresql('3.111.115.15', 'stackexchange', 'posts', 'stack_readonly_user', 'clickhouse');

-- Step 5
SELECT count() FROM posts;
SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'posts', 'stack_readonly_user', 'clickhouse');

-- Step 6
SELECT
    id,
    title,
    name
FROM posts
JOIN post_types
ON posts.post_type_id = post_types.id
WHERE title ilike '%clickhouse%';

-- Step 7
SELECT
    id,
    title,
    dictGet('post_types', 'name', post_type_id)
FROM posts
WHERE title ilike '%clickhouse%';