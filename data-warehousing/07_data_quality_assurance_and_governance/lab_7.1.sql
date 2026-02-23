-- Step 1
CREATE MATERIALIZED VIEW bronze.stg_badges_validator TO bronze.stg_badges
AS
SELECT 
id AS badge_id,
user_id,
name AS badge_name,
date AS badge_awarded_at,
class AS badge_class,
tag_based
FROM sources.badges
WHERE
id > 0
AND user_id > 0
AND name != ''
AND date >= '2008-01-01 00:00:00'  -- Stack Exchange founded in 2008
AND date <= now()
AND class IN (1, 2, 3);  -- Gold=1, Silver=2, Bronze=3

-- Step 2
CREATE OR REPLACE TABLE bronze.stg_badges_invalid
(
badge_id UInt64,
user_id Int64,
badge_name String,
badge_awarded_at DateTime,
badge_class UInt8,
tag_based Bool,
load_timestamp DateTime,
rejection_reason String
)
ENGINE = MergeTree()
ORDER BY (load_timestamp, badge_id);

CREATE MATERIALIZED VIEW bronze.stg_badges_invalid_mv TO bronze.stg_badges_invalid
AS
SELECT 
id AS badge_id,
user_id,
name AS badge_name,
date AS badge_awarded_at,
class AS badge_class,
tag_based,
now() AS load_timestamp,
multiIf(
  id = 0, 'Invalid badge_id: must be > 0',
  user_id <= 0, 'Invalid user_id: must be > 0',
  name = '', 'Invalid badge_name: cannot be empty',
  date < '2008-01-01 00:00:00', 'Invalid badge_awarded_at: before Stack Exchange existed',
  date > now(), 'Invalid badge_awarded_at: future date',
  class NOT IN (1, 2, 3), 'Invalid badge_class: must be 1, 2, or 3',
  'Unknown validation error'
) AS rejection_reason
FROM badges
WHERE NOT (
badge_id > 0
AND user_id > 0
AND badge_name != ''
AND badge_awarded_at >= '2008-01-01 00:00:00'
AND badge_awarded_at <= now()
AND badge_class IN (1, 2, 3)
);


-- Step 3
INSERT INTO sources.badges (id, user_id, name, date, class, tag_based) VALUES
(1, 101, 'Enthusiast', '2023-05-15 14:30:00', 3, false),
(2, 102, 'Nice Answer', '2022-08-20 09:15:00', 3, false),
(14, 113, 'Pioneer', '2007-12-31 23:59:00', 1, false),
(20, 119, 'Platinum Badge', '2023-05-05 10:00:00', 0, false);


-- Step 4
SELECT COUNT(*) as invalid_count FROM bronze.stg_badges_invalid;
