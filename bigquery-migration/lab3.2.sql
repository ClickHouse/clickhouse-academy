--Step 1
CREATE DICTIONARY post_types (
    Id UInt8,
    Name String
)
PRIMARY KEY Id
SOURCE(HTTP(
    url 'https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/post_types/post_types.parquet'
    format 'Parquet'
))
LAYOUT(FLAT())
LIFETIME(3600);

-- Step 2
SELECT * FROM post_types LIMIT 10;

-- Step 3
CREATE OR REPLACE TABLE posts (
    Id Int32,
    PostTypeId UInt8,
    AcceptedAnswerId Nullable(Int32),
    CreationDate DateTime,
    Score Int32,
    ViewCount Nullable(Int32),
    Body String,
    OwnerUserId Nullable(Int32),
    OwnerDisplayName String,
    LastEditorUserId Nullable(Int32),
    LastEditorDisplayName String,
    LastEditDate DateTime,
    LastActivityDate DateTime,
    Title String,
    Tags String,
    AnswerCount Nullable(Int32),
    CommentCount Int32,
    FavoriteCount Nullable(Int32),
    ContentLicense String,
    ParentId Nullable(Int32),
    CommunityOwnedDate DateTime,
    ClosedDate DateTime
)
ENGINE = MergeTree
PRIMARY KEY Id;

-- Step 4
INSERT INTO posts
SELECT * FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/posts/*.parquet');

-- Step 5
SELECT count() FROM posts;
SELECT count() FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/posts/*.parquet');

-- Step 6
SELECT
    Id,
    Title,
    Name
FROM posts
JOIN post_types
ON posts.PostTypeId = post_types.Id
WHERE Title ilike '%clickhouse%';

-- Step 7
SELECT
    Id,
    Title,
    dictGet('post_types', 'Name', PostTypeId)
FROM posts
WHERE Title ilike '%clickhouse%';