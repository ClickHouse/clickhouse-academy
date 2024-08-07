-- Step 3
CREATE TABLE votes (
    Id UInt32,
    PostId UInt32,
    VoteType LowCardinality(String),
    UserId Int32,
    CreationDate DateTime,
    BountyAmount UInt16
)
ENGINE = MergeTree
PRIMARY KEY (VoteType, PostId, CreationDate);

-- Step 4
INSERT INTO votes
SELECT Id, PostId, VoteType, UserId, CreationDate, BountyAmount
FROM(
    SELECT Id, PostId, Name as VoteType, UserId, CreationDate, BountyAmount, VoteTypeId
    FROM gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/votes/2024.parquet') AS v
    JOIN gcs('https://storage.googleapis.com/clickhouse-public-datasets/stackoverflow/parquet/vote_types/vote_types.parquet') AS vt
    ON
        v.VoteTypeId = vt.Id);

-- Step 5
SELECT * FROM votes LIMIT 1;

SELECT
    VoteType,
    count() as c,
    bar(c, 0, 300000)
FROM votes
GROUP BY VoteType
ORDER BY 2 DESC;