-- Step 1

SELECT 
   count() AS num_posts,
   avg(score) AS avg_score
FROM posts
WHERE toYYYYMM(creation_date) = 202401;

-- Step 2
SELECT 
   toYYYYMM(creation_date) AS month,
   count() AS num_posts,
   avg(score) AS avg_score
FROM posts
GROUP BY month
ORDER BY month;


-- Step 3
CREATE TABLE posts_by_month
(
   id UInt64,
   post_type_id UInt8,
   creation_date DateTime,
   score Int32,
   view_count UInt32,
   owner_user_id UInt64,
   title String,
   tags String,
   answer_count UInt16,
   comment_count UInt16
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(creation_date)
ORDER BY (post_type_id, creation_date);


-- Step 4
CREATE MATERIALIZED VIEW posts_by_month_view
TO posts_by_month
AS
SELECT 
   id,
   post_type_id,
   creation_date,
   score,
   view_count,
   owner_user_id,
   title,
   tags,
   answer_count,
   comment_count
FROM posts;


-- Step 5
INSERT INTO posts_by_month
SELECT 
   id,
   post_type_id,
   creation_date,
   score,
   view_count,
   owner_user_id,
   title,
   tags,
   answer_count,
   comment_count
FROM posts;

-- Step 6
SELECT count() FROM posts_by_month;
SELECT count() FROM posts;

-- Step 7
SELECT 
   count() AS num_posts,
   avg(score) AS avg_score
FROM posts_by_month_dest
WHERE toYYYYMM(creation_date) = 202401;
-- Only 104,922 rows were scanned. You should notice that only January’s partition was scanned.

-- Step 8
SELECT 
   count() AS num_questions,
   avg(score) AS avg_score,
   sum(view_count) AS total_views,
   avg(answer_count) AS avg_answers
FROM posts_by_month
WHERE post_type_id = 1
 AND toYYYYMM(creation_date) = 202402;


-- Step 9
INSERT INTO posts (
   id, post_type_id, creation_date, score, view_count, 
   owner_user_id, title, tags, answer_count, comment_count
) VALUES
(99999991, 1, '2024-04-15 10:00:00', 5, 150, 12345678, 
'How to optimize ClickHouse materialized views?', 
'|clickhouse|materialized-views|performance|', 2, 3),
(99999992, 2, '2024-04-15 11:30:00', 10, 0, 87654321, 
'', '', 0, 1),
(99999993, 1, '2024-04-20 09:45:00', 0, 25, 11223344, 
'Understanding incremental materialized views', 
'|clickhouse|database|', 0, 0);

-- verify you have three new rows

SELECT 
   id,
   post_type_id,
   creation_date,
   title,
   score,
   view_count
FROM posts_by_month_dest
WHERE toYYYYMM(creation_date) = 202404
ORDER BY creation_date;


-- Step 10
SELECT * FROM system.parts
WHERE table='posts_by_month';
