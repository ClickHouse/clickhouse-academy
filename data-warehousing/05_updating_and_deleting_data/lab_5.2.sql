-- Step 1
CREATE OR REPLACE TABLE votes_rmt (
    id UInt32,
    post_id UInt32,
    vote_type LowCardinality(String),
    user_id Int32,
    creation_date DateTime,
    bounty_amount UInt16
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

INSERT INTO votes_rmt
SELECT 
    id,
    post_id,
    vote_type_id as vote_type,
    user_id,
    creation_date,
    bounty_amount
FROM sources.votes;

-- Step 2
DESCRIBE votes_rmt;
SELECT * FROM votes_rmt LIMIT 5;


-- Step 3
INSERT INTO votes_rmt (id, post_id, vote_type, user_id, creation_date, bounty_amount) VALUES
(275177253, 77885992, 3, 0, '2024-01-26 00:00:02', 0);


-- Step 4
SELECT * FROM votes_rmt WHERE id = 275177253;


-- Step 5
SELECT * FROM votes_rmt FINAL WHERE id = 275177253;


-- Step 6
CREATE OR REPLACE TABLE votes_rmt2 (
    id UInt32,
    post_id UInt32,
    vote_type LowCardinality(String),
    user_id Int32,
    creation_date DateTime,
    bounty_amount UInt16
    version UInt32
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

INSERT INTO votes_rmt
SELECT 
    id,
    post_id,
    vote_type_id as vote_type,
    user_id,
    creation_date,
    bounty_amount
    1
FROM sources.votes;


-- Step 7
INSERT INTO votes_rmt2 (id, post_id, vote_type, user_id, creation_date, bounty_amount, version) VALUES
(275177253, 77885992, 3, 0, '2024-01-26 00:00:02', 10);
(275177253, 77885992, 3, 0, '2024-01-26 00:00:04', 5);


-- Step 8
SELECT * FROM votes_rmt2 FINAL WHERE id = 275177253;


-- Step 9
OPTIMIZE TABLE rates_monthly2 FINAL;
SELECT * votes_rmt2 FINAL WHERE id = 275177253;

