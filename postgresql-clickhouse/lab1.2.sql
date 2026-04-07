-- Step 1
CREATE TABLE badges
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

-- Step 2
SELECT count(*) FROM badges;
SELECT count(*) FROM postgresql('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

-- Step 3
SHOW CREATE TABLE badges format Vertical;

-- Step 4

-- posts
CREATE TABLE posts
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'posts', 'stack_readonly_user', 'clickhouse');

-- users
CREATE TABLE users
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'users', 'stack_readonly_user', 'clickhouse');

--votes
CREATE TABLE votes
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'votes', 'stack_readonly_user', 'clickhouse');

-- post_types
CREATE TABLE vote_types
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'vote_types', 'stack_readonly_user', 'clickhouse');

-- votes_types
CREATE TABLE post_types
ORDER BY id AS
SELECT *
FROM postgresql('3.111.115.15', 'stackexchange', 'post_types', 'stack_readonly_user', 'clickhouse');

-- Check the sizes of each one of the table to see if it is correct
SELECT
  (SELECT count() FROM badges) AS badges,
  (SELECT count() FROM posts) AS posts,
  (SELECT count() FROM users) AS users,
  (SELECT count() FROM votes) AS votes,
  (SELECT count() FROM vote_types) AS vote_types,
  (SELECT count() FROM post_types) AS post_types
FORMAT Vertical;

-- Step 5 (users with more than 10 questions which receive the most views)
SELECT owner_display_name, SUM(view_count) AS total_views
FROM posts
JOIN post_types
ON posts.post_type_id = post_types.id
WHERE (post_types.name = 'Question') AND (owner_display_name != '')
GROUP BY owner_display_name
HAVING COUNT(*) > 10
ORDER BY total_views DESC
LIMIT 5;

-- Step 6 (which question tags receive the most views)
WITH tags_exploded AS (
    SELECT
        arrayJoin(splitByChar('|', assumeNotNull(tags))) AS tag,
        COALESCE(view_count, 0) AS view_count
    FROM posts
    JOIN post_types
    ON posts.post_type_id = post_types.id
    WHERE tags <> '' AND post_types.name = 'Question'
),
filtered_tags AS (
    SELECT
        tag,
        view_count
    FROM tags_exploded
    WHERE tag <> ''
)
SELECT
    tag AS tags,
    SUM(view_count) AS views
FROM filtered_tags
GROUP BY tag
ORDER BY views DESC
LIMIT 5;

-- Step 7 (most viewed question of each year)
WITH yearly_views AS (
    SELECT
        EXTRACT(YEAR FROM creation_date) AS year,
        title,
        view_count,
        ROW_NUMBER() OVER (
            PARTITION BY EXTRACT(YEAR FROM creation_date)
            ORDER BY view_count DESC
        ) AS rn
    FROM posts
    JOIN post_types
    ON posts.post_type_id = post_types.id
    WHERE post_types.name = 'Question'
)
SELECT
    year,
    title AS most_viewed_question_title,
    view_count AS max_view_count
FROM yearly_views
WHERE rn = 1
ORDER BY year;

-- Step 8 (Tags with more than 100 occurrences with the largest percentage
-- increase from January 2024 to February 2024)
SELECT
    tag,
    SUM(CASE WHEN month = 2 THEN count ELSE 0 END) AS count_feb,
    SUM(CASE WHEN month = 1 THEN count ELSE 0 END) AS count_jan,
    (
        (
            SUM(CASE WHEN month = 2 THEN count ELSE 0 END)
          - SUM(CASE WHEN month = 1 THEN count ELSE 0 END)
        )
        / SUM(CASE WHEN month = 1 THEN count ELSE 0 END)::float
    ) * 100 AS percent_change
FROM (
    SELECT
        arrayJoin(splitByChar('|', assumeNotNull(tags))) AS tag,
        EXTRACT(MONTH FROM creation_date) AS month,
        COUNT(*) AS count
    FROM posts
    WHERE EXTRACT(YEAR FROM creation_date) = 2024
      AND EXTRACT(MONTH FROM creation_date) IN (1, 2)
      AND tags <> ''
    GROUP BY tag, month
) AS monthly_counts
GROUP BY tag
HAVING SUM(CASE WHEN month = 1 THEN count ELSE 0 END) > 100
   AND SUM(CASE WHEN month = 2 THEN count ELSE 0 END) > 100
ORDER BY percent_change DESC
LIMIT 5;

-- Step 9 (most voted post with 'clickhouse' in the title, including post and user details)
SELECT
    users.id AS user_id,
    users.display_name AS user_name,
    posts.id AS post_id,
    posts.title AS post_title,
    COUNT(votes.id) AS vote_count
FROM
    posts
JOIN
    votes ON posts.id = votes.post_id
JOIN
    users ON posts.owner_user_id = users.id
WHERE
    posts.title ILIKE '%clickhouse%'
GROUP BY
    users.id, users.display_name, posts.id, posts.title
ORDER BY
    vote_count DESC
LIMIT 1;
