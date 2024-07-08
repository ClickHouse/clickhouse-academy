-- Step 1
CREATE USER sara IDENTIFIED BY 'password';

-- Step 2
CREATE ROLE developer;
GRANT SELECT, INSERT ON my_db.* TO developer;

-- Step 3
GRANT developer TO sara;

-- Step 4
./clickhouse client --user=sara

-- Step 6
CREATE SETTINGS PROFILE lab3_settings_profile
SETTINGS
   max_result_rows = 100 MIN 1 MAX 100
TO developer;

-- Step 7
select * from my_db.events;