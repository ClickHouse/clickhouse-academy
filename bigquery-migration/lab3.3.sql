--Step 1:
CREATE TABLE post_votes (
  PostId UInt32,
  UpVotes SimpleAggregateFunction(sum, Int64),
  DownVotes SimpleAggregateFunction(sum, Int64),
  Score SimpleAggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY PostId;

--Step 2:
CREATE MATERIALIZED VIEW post_votes_MV
TO post_votes
AS
  SELECT
      PostId,
      sumSimpleState(up) as UpVotes,
      sumSimpleState(down) as DownVotes,
      sumSimpleState(up - down) AS Score
  FROM (
      SELECT
          PostId,
          countIf(VoteType = 'UpMod') AS up,
          countIf(VoteType = 'DownMod') AS down
      FROM votes
      GROUP BY PostId
  ) GROUP BY PostId;

--Step 3:
INSERT INTO post_votes
   SELECT
      PostId,
      sumSimpleState(up) as UpVotes,
      sumSimpleState(down) as DownVotes,
      sumSimpleState(up - down) AS Score
  FROM (
      SELECT
          PostId,
          countIf(VoteType = 'UpMod') AS up,
          countIf(VoteType = 'DownMod') AS down
      FROM votes
      GROUP BY PostId
  ) GROUP BY PostId;

--Step 4:
SELECT
  PostId, sum(UpVotes), sum(DownVotes), sum(Score)
FROM post_votes
GROUP BY PostId
ORDER BY 2 DESC
LIMIT 20;

--Step 5:
SELECT * FROM post_votes WHERE PostId = 78163859;


--Step 6:
INSERT INTO votes VALUES
(300000000, 78163859, 'UpMod', 114, '2024-07-01 10:00:00', 0)
(300000001, 78163859, 'UpMod', 114, '2024-07-01 10:00:01', 0)
(300000002, 78163859, 'UpMod', 114, '2024-07-01 10:00:02', 0)
(300000003, 78163859, 'UpMod', 114, '2024-07-01 10:00:02', 0)
(300000004, 78163859, 'DownMod', 114, '2024-07-01 10:00:05', 0);


--Step 7:
SELECT * FROM post_votes WHERE PostId = 78163859;


--Step 8:
SELECT
  PostId, sum(UpVotes), sum(DownVotes), sum(Score)
FROM post_votes
GROUP BY PostId
ORDER BY 2 DESC
LIMIT 20;