-- Step 3

CREATE TABLE denormalized_posts
(
    id                         UInt32,
    post_type                  LowCardinality(String),
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
    content_license            String,
    closed_date                DateTime,
    community_owned_date       DateTime
)
ENGINE = MergeTree
PRIMARY KEY (post_type, owner_display_name, creation_date);

-- Step 4

INSERT INTO denormalized_posts
SELECT
    id, post_types.name, accepted_answer_id, parent_id, creation_date, deletion_date, score, view_count, owner_user_id, owner_display_name, last_editor_user_id, last_editor_display_name, last_edit_date, last_activity_date, title, tags, answer_count, comment_count, favorite_count, content_license, closed_date, community_owned_date
FROM
    posts
JOIN
    post_types
ON
    posts.post_type_id = post_types.id;

--Step 5

SELECT * FROM denormalized_posts LIMIT 1;

SELECT
    post_type,
    count() as c,
    bar(c, 0, 300000)
FROM denormalized_posts
GROUP BY post_type
ORDER BY 2 DESC;

-- Step 6
SELECT
    owner_display_name,
    SUM(view_count) AS total_views
FROM denormalized_posts
WHERE (post_type = 'Question') AND (owner_display_name != '')
GROUP BY owner_display_name
HAVING COUNT(*) > 10
ORDER BY total_views DESC
LIMIT 5;


