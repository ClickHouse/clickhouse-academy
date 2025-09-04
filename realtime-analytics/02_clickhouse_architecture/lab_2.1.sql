-- Step 1
SELECT * 
FROM system.parts
WHERE table = 'uk_prices_1'
AND active = 1;

-- Step 2
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'uk_prices_1' AND active = 1;

-- Step 4
SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE town = 'LONDON';

-- Step 5
SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE toYYYYMM(date) = '202207';

-- Step 6
CREATE TABLE uk_prices_2
(
    `id` Nullable(String),
    `price` Nullable(String),
    `date` DateTime,
    `postcode` String,
    `type` Nullable(String),
    `is_new` Nullable(String),
    `duration` Nullable(String),
    `addr1` Nullable(String),
    `addr2` Nullable(String),
    `street` Nullable(String),
    `locality` Nullable(String),
    `town` Nullable(String),
    `district` Nullable(String),
    `county` Nullable(String),
    `column15` Nullable(String),
    `column16` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY (postcode, date);

-- Step 7
INSERT INTO uk_prices_2
    SELECT * FROM uk_prices_1;

-- Step 8
SELECT
    max(toUInt32(price))
FROM uk_prices_2
WHERE postcode = 'LU1 5FT';

-- Step 9
SELECT avg(toUInt32(price))
FROM uk_prices_2
WHERE toYear(date) >= '2020';

-- Step 10
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'uk_prices_2' AND active = 1;