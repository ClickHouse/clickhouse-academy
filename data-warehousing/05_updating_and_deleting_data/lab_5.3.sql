-- Step 1
CREATE OR REPLACE TABLE users_cmt(
   user_id Int32,
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
   account_id String,
   sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY user_id;


-- Step 2
INSERT INTO users_cmt VALUES
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
'http://www.circleprefect.com', 'US Western States', '', 27, 0, 0, '3394275', 1),

(5012345, 150, '2015-03-15 10:30:00', 'CodeMaster', '2024-04-08 14:22:15',
'https://github.com/codemaster', 'San Francisco, CA', 'Software Engineer', 542, 15, 3, '4512678', 1),

(6789012, 87, '2016-07-22 08:15:30', 'DataWizard', '2024-04-07 09:45:00',
'', 'London, UK', 'Data Analyst', 234, 8, 1, '5678901', 1);


-- Step 3
SELECT * FROM users_cmt;


-- Step 4
INSERT INTO users_cmt VALUES
-- Cancel the old state (sign = -1)
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
'http://www.circleprefect.com', 'US Western States', '', 27, 0, 0, '3394275', -1),

-- Insert new state (sign = 1) with updated up_votes
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
'http://www.circleprefect.com', 'US Western States', '', 27, 1, 0, '3394275', 1);


-- Step 5
SELECT * FROM users_cmt
WHERE user_id = 2848360;
--Answer: There should be 3 values. Two with a sign of 1, and one with a sign of -1. 


-- Step 6
SELECT * FROM users_cmt FINAL
WHERE user_id = 2848360;
-- Answer: Only the most recently inserted row is returned.

-- Step 7
CREATE OR REPLACE TABLE users_vcmt(
   user_id Int32,
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
   account_id String,
   sign Int8,
   version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
PRIMARY KEY user_id
ORDER BY user_id;


-- Step 8
INSERT INTO users_vcmt VALUES
-- Initial state (version 1)
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
 'http://www.website.com', 'US Western States', '', 27, 0, 0, '2848360', 1, 1),

-- Cancel version 1
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
 'http://www.website.com', 'US Western States', '', 27, 0, 0, '2848360', -1, 1),

-- Insert version 2
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
 'http://www.website.com', 'US Western States', '', 27, 5, 0, '2848360', 1, 2),

-- Cancel version 2
(2848360, 3, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
 'http://www.website.com', 'US Western States', '', 27, 5, 0, '2848360', -1, 2),

-- Insert version 3
(2848360, 50, '2013-10-04 23:01:19', 'MRBaird', '2024-04-06 21:10:08', 
 'http://www.website.com', 'US Western States', '', 27, 5, 0, '2848360', 1, 3);


-- Step 9
SELECT 
*
FROM users_vcmt
WHERE user_id = 2848360;

SELECT 
*
FROM users_vcmt FINAL 
WHERE user_id = 2848360;




