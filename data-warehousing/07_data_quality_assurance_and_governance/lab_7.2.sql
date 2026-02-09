-- Step 1
CREATE ROLE data_engineer;

GRANT SELECT ON *.* TO data_engineer;
GRANT INSERT ON *.* TO data_engineer;
GRANT CREATE ON *.* TO data_engineer;
GRANT ALTER ON *.* TO data_engineer;
GRANT DROP ON *.* TO data_engineer;
GRANT TRUNCATE ON *.* TO data_engineer;
GRANT SHOW ON *.* TO data_engineer;
GRANT OPTIMIZE ON *.* TO data_engineer;
GRANT SYSTEM ON *.* TO data_engineer;
GRANT dictGet ON *.* TO data_engineer;


-- Step 2
CREATE SETTINGS PROFILE etl_profile
SETTINGS 
  max_memory_usage = 20000000000,  -- 20GB
  max_execution_time = 3600,        -- 1 hour
  max_threads = 8
TO data_engineer;


-- Step 3
CREATE QUOTA engineer_quota
FOR INTERVAL 1 hour MAX queries = 1, 
FOR INTERVAL 1 day MAX query_selects = 5000
TO data_engineer;

-- Step 4
SELECT currentRoles();

-- Step 5
GRANT data_engineer to [your_user_name]
SET ROLE data_engineer;


-- Step 6
-- your previous role was most likely sql_console_admin
SET ROLE sql_console_admin
