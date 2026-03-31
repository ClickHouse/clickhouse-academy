-- Step 1

-- Query 1 denormalized
SELECT
    owner_display_name,
    SUM(view_count) AS total_views
FROM denormalized_posts
WHERE (post_type = 'Question') AND (owner_display_name != '')
GROUP BY owner_display_name
HAVING COUNT(*) > 10
ORDER BY total_views DESC
LIMIT 5;

-- Step 2 

-- Query 2 original
WITH tags_exploded AS (
    SELECT
        arrayJoin(splitByChar('|', tags)) AS tag,
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

-- Query 2 optimized
SELECT
    arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', tags))) AS tags,
    sum(view_count) AS views
FROM posts
JOIN post_types
ON posts.post_type_id = post_types.id
WHERE post_types.name = 'Question'
GROUP BY tags
ORDER BY views DESC
LIMIT 5;

-- Query 2 optimized and denormalized
SELECT
    arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', tags))) AS tags,
    sum(view_count) AS views
FROM denormalized_posts
WHERE post_type = 'Question'
GROUP BY tags
ORDER BY views DESC
LIMIT 5;

-- Step 3

-- Query 3 original
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

-- Query 3 optimized
SELECT
    toYear(creation_date) AS year,
    argMax(title, view_count) AS most_viewed_question_title,
    max(view_count) AS max_view_count
FROM posts
JOIN post_types
ON posts.post_type_id = post_types.id
WHERE post_types.name = 'Question'
GROUP BY year
ORDER BY year ASC;

-- Query 3 optimized and denormalized
SELECT
    toYear(creation_date) AS year,
    argMax(title, view_count) AS most_viewed_question_title,
    max(view_count) AS max_view_count
FROM denormalized_posts
WHERE post_type = 'Question'
GROUP BY year
ORDER BY year ASC;

-- Step 4

-- Query 4 original
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
        arrayJoin(splitByChar('|', tags))AS tag,
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

-- Query 4 optimized
SELECT
    arrayJoin(arrayFilter(t -> (t != ''), splitByChar('|', tags))) AS tag,
    countIf(toStartOfMonth(creation_date) = '2024-02-01') AS count_feb,
    countIf(toStartOfMonth(creation_date) = '2024-01-01') AS count_jan,
    ((count_feb - count_jan) / count_jan) * 100 AS percent_change
FROM posts
WHERE toYear(creation_date) = 2024
GROUP BY tag
HAVING (count_jan > 100) AND (count_feb > 100)
ORDER BY percent_change DESC
LIMIT 5;

-- Step 5

-- Query 5 original
SELECT
    u.id AS user_id,
    u.display_name AS user_name,
    p.id AS post_id,
    p.title AS post_title,
    COUNT(v.id) AS vote_count
FROM
    posts p
JOIN
    votes v ON p.id = v.post_id
JOIN
    users u ON p.owner_user_id = u.id
WHERE
    p.title ILIKE '%clickhouse%'
GROUP BY
    u.id, u.display_name, p.id, p.title
ORDER BY
    vote_count DESC
LIMIT 1;

-- Query 5 optimized
WITH filtered_posts AS (
    SELECT id AS post_id, owner_user_id, title
    FROM posts
    WHERE title ILIKE '%clickhouse%'
),
post_votes AS (
    SELECT post_id, COUNT(*) AS vote_count
    FROM votes
    GROUP BY post_id
)
SELECT 
    u.id AS user_id,
    u.display_name AS user_name,
    fp.post_id,
    fp.title AS post_title,
    pv.vote_count
FROM 
    filtered_posts fp
LEFT JOIN 
    post_votes pv ON fp.post_id = pv.post_id
LEFT JOIN 
    users u ON fp.owner_user_id = u.id
ORDER BY 
    pv.vote_count DESC
LIMIT 1;
