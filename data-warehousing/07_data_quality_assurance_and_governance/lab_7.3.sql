-- Step 1
CREATE ROLE role_marketing_analyst;

GRANT SELECT(
  user_id,
  display_name,
  reputation,
  user_created_at,
  last_access_at,
  days_since_last_access,
  profile_views,
  website_url,
  -- No location 
  total_questions_asked,
  total_answers_posted,
  total_posts,
  avg_question_score,
  avg_answer_score,
  total_accepted_answers,
  total_post_views,
  total_votes_received,
  upvotes_received,
  downvotes_received,
  total_badges,
  gold_badges,
  silver_badges,
  bronze_badges,
  is_active_user,
  is_power_user
) ON gold.dim_user_activity_marketing TO role_marketing_analyst;


-- Step 2
GRANT role_marketing_analyst TO `your_username_here`;
SET ROLE role_marketing_analyst;
SELECT * FROM gold.dim_user_activity_marketing LIMIT 5;


-- Step 3
-- you were most likely previously a sql_console_admin
SET ROLE sql_console_admin;

-- Step 4
-- Create the role
CREATE ROLE role_marketing_analyst_USA;

-- Grant column-level permissions
GRANT SELECT(
    user_id,
    display_name,
    reputation,
    user_created_at,
    last_access_at,
    days_since_last_access,
    location,  
    profile_views,
    
    -- Posts metrics
    total_questions_asked,
    total_answers_posted,
    total_posts,
    avg_question_score,
    avg_answer_score,
    total_accepted_answers,
    total_post_views,
    
    -- Voting metrics
    total_votes_received,
    upvotes_received,
    downvotes_received,
    
    -- Badge metrics
    total_badges,
    gold_badges,
    silver_badges,
    bronze_badges,
    
    -- Activity flags
    is_active_user,
    is_power_user
) ON gold.dim_user_activity_marketing TO role_marketing_analyst_USA;

-- Create a row policy for USA-based users
CREATE ROW POLICY policy_usa_only 
ON gold.dim_user_activity_marketing
FOR SELECT
USING location LIKE '%USA%' OR location LIKE '%United States%'
TO role_marketing_analyst_USA;


-- Step 5
GRANT role_marketing_analyst_USA TO `your-username-here`;
SET ROLE role_marketing_analyst_USA;
SELECT distinct(location) FROM gold.dim_user_activity_marketing;


-- Step 6
-- you were most likely previously a sql_console_admin
SET ROLE sql_console_admin;
