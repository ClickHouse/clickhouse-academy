-- Step 1
WITH toStartOfMonth(created_at) AS month
SELECT
  month,
  count() AS post_volume,
  round(avg(score), 2) AS avg_score
FROM gold.fact_post_performance
GROUP BY month
ORDER BY month DESC;

WITH toStartOfMonth(created_at) AS month
SELECT
  month,
  round(avg(view_count), 2) AS avg_views,
  round(avg(answer_count), 2) AS avg_answers
FROM gold.fact_post_performance
WHERE is_question = 1
GROUP BY month
ORDER BY month DESC;

-- Step 2
CREATE TABLE post_performance_aggs (
  month Date,
  performance_tier String,
  post_volume AggregateFunction(count),
  avg_score AggregateFunction(avg, Int32),
  avg_view_count AggregateFunction(avg, UInt32),
  avg_answer_count AggregateFunction(avg, UInt16),
  max_score SimpleAggregateFunction(max, Int32),
  min_score SimpleAggregateFunction(min, Int32)
)
ENGINE = AggregatingMergeTree()
PRIMARY KEY (month, performance_tier);

CREATE MATERIALIZED VIEW post_performance_aggs_mv
TO post_performance_aggs
AS
  WITH toStartOfMonth(created_at) AS month
  SELECT
      month,
      countState() AS post_volume,
      performance_tier,
      avgState(score) AS avg_score,
      avgState(view_count) AS avg_view_count,
      avgState(answer_count) AS avg_answer_count,
      maxSimpleState(score) AS max_score,
      minSimpleState(score) AS min_score
  FROM gold.fact_post_performance
  GROUP BY month, performance_tier;

-- Step 3
INSERT INTO post_performance_aggs
  WITH toStartOfMonth(created_at) AS month
  SELECT
      month,
      performance_tier,
      countState() AS post_volume,
      avgState(score) AS avg_score,
      avgState(view_count) AS avg_view_count,
      avgState(answer_count) AS avg_answer_count,
      maxSimpleState(score) AS max_score,
      minSimpleState(score) AS min_score
  FROM gold.fact_post_performance
  WHERE created_at >= toDate('2020-01-01')
  GROUP BY month, performance_tier;


-- Step 4
SELECT * FROM post_performance_aggs;

-- Step 5
SELECT
  countMerge(post_volume) AS total_posts,
  round(avgMerge(avg_view_count), 2) AS avg_views,
  round(avgMerge(avg_answer_count), 2) AS avg_answers,
  max(max_score) AS highest_score,
  min(min_score) AS lowest_score
FROM post_performance_aggs;

