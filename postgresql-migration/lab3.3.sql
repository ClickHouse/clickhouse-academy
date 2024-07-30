--Step 1:
CREATE TABLE post_votes (
  post_id UInt32,
  up_votes SimpleAggregateFunction(sum, Int64),
  down_votes SimpleAggregateFunction(sum, Int64),
  score SimpleAggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY post_id;

--Step 2:
CREATE MATERIALIZED VIEW post_votes_MV
TO post_votes
AS
  SELECT
      post_id,
      sumSimpleState(up) as up_votes,
      sumSimpleState(down) as down_votes,
      sumSimpleState(up - down) AS score
  FROM (
      SELECT
          post_id,
          countIf(vote_type = 'UpMod') AS up,
          countIf(vote_type = 'DownMod') AS down
      FROM votes
      GROUP BY post_id
  ) GROUP BY post_id;

--Step 3:
INSERT INTO post_votes
   SELECT
      post_id,
      sumSimpleState(up) as up_votes,
      sumSimpleState(down) as down_votes,
      sumSimpleState(up - down) AS score
  FROM (
      SELECT
          post_id,
          countIf(vote_type = 'UpMod') AS up,
          countIf(vote_type = 'DownMod') AS down
      FROM votes
      GROUP BY post_id
  ) GROUP BY post_id;

--Step 4:
SELECT
  post_id, sum(up_votes), sum(down_votes), sum(score)
FROM post_votes
GROUP BY post_id
ORDER BY 2 DESC
LIMIT 10;

--Step 5:
SELECT * FROM post_votes WHERE post_id = 78163859;


--Step 6:
INSERT INTO votes VALUES
(300000000, 78163859, 'UpMod', 114, '2024-07-01 10:00:00', 0)
(300000001, 78163859, 'UpMod', 114, '2024-07-01 10:00:01', 0)
(300000002, 78163859, 'UpMod', 114, '2024-07-01 10:00:02', 0)
(300000003, 78163859, 'UpMod', 114, '2024-07-01 10:00:02', 0)
(300000004, 78163859, 'DownMod', 114, '2024-07-01 10:00:05', 0);


--Step 7:
SELECT * FROM post_votes WHERE post_id = 78163859;


--Step 8:
SELECT post_id, sum(up_votes), sum(down_votes), sum(score) FROM post_votes GROUP BY post_id ORDER BY 2 DESC;