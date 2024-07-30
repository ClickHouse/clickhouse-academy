-- Step 3
CREATE TABLE votes (
    id UInt32,
    post_id UInt32,
    vote_type LowCardinality(String),
    user_id Int32,
    creation_date DateTime,
    bounty_amount UInt16
)
ENGINE = MergeTree
PRIMARY KEY (vote_type, post_id, creation_date);

-- Step 4
INSERT INTO votes
SELECT
    id, post_id, postgres_vote_types.name, user_id,
    creation_date, bounty_amount
FROM
    postgresql('3.111.115.15', 'stackexchange', 'votes', 'stack_readonly_user', 'clickhouse') as postgres_votes
JOIN
    postgresql('3.111.115.15', 'stackexchange', 'vote_types', 'stack_readonly_user', 'clickhouse') as postgres_vote_types
ON
    postgres_votes.vote_type_id = postgres_vote_types.id;

-- Step 5
SELECT * FROM votes LIMIT 1;

SELECT
    vote_type,
    count() as c,
    bar(c, 0, 300000)
FROM votes
GROUP BY vote_type
ORDER BY 2 DESC;