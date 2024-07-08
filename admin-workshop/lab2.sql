-- Step 3
CREATE DATABASE my_db;

-- Step 4
CREATE TABLE my_db.events
(
    device_id UInt32,
    event_type String,
    value UInt64
)
ENGINE = MergeTree
PRIMARY KEY (device_id, event_type);

-- Step 5:
-- A symbolic link named events was created in the my_db folder. The symbolic link points to a subfolder of ~/workshop/store, which is where the data for the events table will actually be stored.

-- Step 6
SELECT *
FROM system.tables
WHERE database = 'my_db'
FORMAT Vertical;

-- Step 10
USE my_db;

-- Step 11
INSERT INTO events VALUES
   (101, 'time', 3874),
   (234, 'disk', 423687),
   (101, 'disk', 789432);

-- Step 12
INSERT INTO events VALUES
   (101, 'time', 4330),
   (101, 'disk', 723488),
   (234, 'cpu', 40),
   (55, 'cpu', 12);

-- Step 13
SELECT * FROM events;

-- Step 16
-- The folder name contains the partition value, but my_db.events did not have a partition key. If a MergeTree table does not use partitioning, then every part folder will start with the name "all".

-- Step 17
-- The 1_1 represents a range of block numbers. In this example, there is only 1 block in the part and is block 1. The other folder 2_2_0 is also one block, block 2.

-- Step 18
-- The 0 represents the "level" of the part in terms of the number of times that part has merged with other parts. A 0 means the part has not merged yet with another part.

-- Step 19
OPTIMIZE TABLE events FINAL;

-- Step 22
SELECT *
FROM events

-- Step 23
SELECT max(value)
FROM events;

-- Step 25
SELECT *
FROM system.parts
WHERE active = 1
FORMAT Vertical;

-- Step 27
-- There are not enough rows to store in a Wide format, so the events table is stored in the Compact format. All the column data is stored in a single file named data.bin, which you can see in your part folder.

-- Step 28
INSERT INTO events
   SELECT *
   FROM generateRandom('device_id UInt32, event_type String, value UInt64')
   LIMIT 1000000;

-- Step 31
CREATE TABLE my_db.events_by_month
(
    device_id UInt32,
    event_type String,
    value UInt64,
    timestamp DateTime
)
ENGINE = MergeTree
PRIMARY KEY (device_id, event_type)
PARTITION BY toYYYYMM(timestamp)
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Step 32
INSERT INTO events_by_month
   SELECT
      *,
      now() - INTERVAL rand()%6 MONTH AS timestamp
   FROM generateRandom('device_id UInt32, event_type String, value UInt64')
   LIMIT 1000000;

-- Step 35
SELECT partition
FROM system.parts
WHERE `table` = 'events_by_month';

-- Step 36
ALTER TABLE my_db.events_by_month DROP PARTITION 202402;

-- Step 37
SELECT count()
FROM my_db.events_by_month;

-- Step 38
CREATE TABLE my_db.new_events
(
    device_id UInt32,
    event_type String,
    value UInt64,
    timestamp DateTime
)
ENGINE = MergeTree
PRIMARY KEY (device_id, event_type)
PARTITION BY toYYYYMM(timestamp)
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Step 39
SELECT count()
FROM my_db.new_events;

-- Step 41
ALTER TABLE my_db.new_events ATTACH PARTITION 202405;

-- Step 42
SELECT count()
FROM my_db.new_events;

