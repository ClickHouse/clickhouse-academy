-- Step 1
CREATE TABLE posts_daily_stats
(
   post_date Date,
   num_posts UInt32,
   avg_score Float64,
   total_views UInt64,
   num_questions UInt32,
   num_answers UInt32
)
ENGINE = MergeTree
ORDER BY post_date;


CREATE MATERIALIZED VIEW posts_daily_stats_mv
REFRESH EVERY 12 HOUR
TO posts_daily_stats
AS
SELECT 
   toDate(creation_date) AS post_date,
   count() AS num_posts,
   avg(score) AS avg_score,
   sum(view_count) AS total_views,
   countIf(post_type_id = 1) AS num_questions,
   countIf(post_type_id = 2) AS num_answers
FROM posts
WHERE toDate(creation_date) >= '2024-01-01'
GROUP BY post_date;


-- Step 2
SELECT 
   post_date,
   num_posts,
   round(avg_score, 2) AS avg_score,
   total_views,
   num_questions,
   num_answers
FROM posts_daily_stats
ORDER BY post_date;


-- Step 3
INSERT INTO posts (
   id, post_type_id, creation_date, score, view_count, 
   owner_user_id, title, tags, answer_count, comment_count
) VALUES
(99999994, 1, '2024-01-19 23:00:00', 15, 500, 11111111, 
'How to use refreshable materialized views?', 
'|clickhouse|materialized-views|', 3, 2),
(99999995, 2, '2024-01-19 23:15:00', 8, 0, 22222222, 
'', '', 0, 0),
(99999996, 1, '2024-01-19 23:30:00', 3, 100, 33333333, 
'Understanding refresh intervals in ClickHouse', 
'|clickhouse|performance|', 1, 0);


-- Step 4
SELECT 
   post_date,
   num_posts,
   round(avg_score, 2) AS avg_score,
   total_views,
   num_questions,
   num_answers
FROM posts_daily_stats
WHERE post_date = '2024-01-19';


-- Step 5
SYSTEM REFRESH VIEW posts_daily_stats_mv;


-- Step 6
SELECT 
   post_date,
   num_posts,
   round(avg_score, 2) AS avg_score,
   total_views,
   num_questions,
   num_answers
FROM posts_daily_stats
WHERE post_date = '2024-01-19';


-- Step 7
SELECT 
   database,
   view,
   status,
   last_refresh_time,
   next_refresh_time,
   exception
FROM system.view_refreshes
WHERE view = 'posts_daily_stats_mv';
