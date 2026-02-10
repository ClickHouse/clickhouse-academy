-- Step  1
CREATE OR REPLACE TABLE users_temp(
    id Int32,
    reputation Int32,
    creation_date DateTime,
    display_name String,
    last_access_date DateTime,
    website_url String,
    location String,
    about_me String,
    views Int64,
    up_votes Int32,
    down_votes Int32,
    account_id String
)
ENGINE = MergeTree
PRIMARY KEY id;

INSERT INTO users_temp
SELECT 
    id,
    reputation,
    creation_date,
    display_name,
    last_access_date,
    website_url,
    location,
    about_me,
    views,
    up_votes,
    down_votes,
    account_id
FROM sources.users;

-- Step 2
ALTER TABLE users_temp DELETE WHERE reputation < 100;

SELECT 
  command,
  is_done,
  parts_to_do,
  latest_fail_reason
FROM system.mutations
WHERE table = 'users_temp'
FORMAT Vertical;
--Answer: You should see that is_done=0 and parts_to_do is nonzero. 


-- Step 3
SELECT 
  command,
  is_done,
  parts_to_do,
  latest_fail_reason
FROM system.mutations
WHERE table = 'users_temp'
FORMAT Vertical;
-- Answer: You should now see parts_to_do is zero and is_done is nonzero!

-- Step 4
UPDATE users_temp 
SET display_name = 'Rex Matthews' 
WHERE id = 67;


-- Step 5
SELECT id, display_name, reputation 
FROM users_temp 
WHERE id = 67;



