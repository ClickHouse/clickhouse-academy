-- Step 5
SELECT count()
FROM postgresql('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

-- Step 6
DESCRIBE TABLE postgresql('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

-- Step 7
CREATE TABLE postgres_badges (
    id Int32,
    user_id Int32,
    name String,
    date DateTime64(6),
    class Int16,
    tag_based UInt8
)
ENGINE = PostgreSQL('3.111.115.15', 'stackexchange', 'badges', 'stack_readonly_user', 'clickhouse');

SELECT count()
FROM postgres_badges;

-- Step 8
SELECT
    (
    SELECT count()
    FROM postgresql('3.111.115.15', 'stackexchange', 'posts', 'stack_readonly_user', 'clickhouse')
    ) AS posts,
    (
    SELECT count()
    FROM postgresql('3.111.115.15', 'stackexchange', 'users', 'stack_readonly_user', 'clickhouse')
    ) AS users,
    (
    SELECT count()
    FROM postgresql('3.111.115.15', 'stackexchange', 'votes', 'stack_readonly_user', 'clickhouse')
    ) AS votes,
    (
    SELECT count()
    FROM postgresql('3.111.115.15', 'stackexchange', 'vote_types', 'stack_readonly_user', 'clickhouse')
    ) AS vote_types,
    (
    SELECT count()
    FROM postgresql('3.111.115.15', 'stackexchange', 'post_types', 'stack_readonly_user', 'clickhouse')
    ) AS post_types
FORMAT Vertical;

SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'posts', 'stack_readonly_user', 'clickhouse');

SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'users', 'stack_readonly_user', 'clickhouse');

SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'votes', 'stack_readonly_user', 'clickhouse');

SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'post_types', 'stack_readonly_user', 'clickhouse');

SELECT count() FROM postgresql('3.111.115.15', 'stackexchange', 'vote_types', 'stack_readonly_user', 'clickhouse');