--Step 1:
CREATE OR REPLACE TABLE ttl_demo (
    key UInt32,
    value String,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY key
TTL timestamp + INTERVAL 60 SECOND;

--Step 2:
INSERT INTO ttl_demo VALUES
    (1, 'row1', now()),
    (2, 'row2', now());

--Step 3:
SELECT * FROM ttl_demo;

--Step 5:
ALTER TABLE ttl_demo
MATERIALIZE TTL;

--Step 6:
SELECT * FROM ttl_demo;

--Step 7:
ALTER TABLE ttl_demo
    MODIFY COLUMN value String TTL timestamp +  INTERVAL 15 SECOND;

--Step 8:
INSERT INTO ttl_demo VALUES
    (1, 'row1', now()),
    (2, 'row2', now());

--Step 9:
ALTER TABLE ttl_demo
MATERIALIZE TTL;

--Step 10:
SELECT * FROM ttl_demo;

--Step 11:
ALTER TABLE ttl_demo
MATERIALIZE TTL;

--Step 12:
ALTER TABLE prices_1
    MODIFY TTL date + INTERVAL 5 YEAR;

--Step 13:
ALTER TABLE prices_1
MATERIALIZE TTL;

--Step 14:
SELECT min(date) FROM prices_1;
