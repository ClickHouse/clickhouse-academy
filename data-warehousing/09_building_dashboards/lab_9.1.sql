-- Step 3
-- Weekly Activity Summary
SELECT 
  toStartOfWeek(activity_date) as week,
  sum(total_questions) as questions,
  sum(total_answers) as answers,
  sum(new_users) as new_users,
  avg(avg_question_score) as avg_score
FROM gold.fct_activity_by_time
WHERE activity_date >= '2024-01-14' - INTERVAL 12 WEEK
GROUP BY week
ORDER BY week;

-- Step 7
-- Aggregate post statistics
SELECT
  countMerge(post_volume) AS total_posts,
  round(avgMerge(avg_view_count), 2) AS avg_views,
  round(avgMerge(avg_answer_count), 2) AS avg_answers,
  max(max_score) AS highest_score,
  min(min_score) AS lowest_score
FROM post_performance_aggs

