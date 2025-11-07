--Step 1:
SELECT
    formatReadableSize(sum(data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio,
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_prices_3' AND active = 1;

--Step 2:
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'uk_prices_3' AND active = 1
GROUP BY column;

--Step 4:
CREATE TABLE prices_1
(
    `id`    UUID,
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String) ,
    `postcode2` LowCardinality(String),
    `type` Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    `is_new` UInt8,
    `duration` Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date)
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_1
    SELECT * FROM uk_prices_3;

--Step 5:
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_1' AND active = 1
GROUP BY column;

--Step 8:
CREATE OR REPLACE TABLE prices_2
(
    `price` UInt32 CODEC(T64, LZ4),
    `date` Date CODEC(DoubleDelta, ZSTD),
    `postcode1` String,
    `postcode2` String,
    `is_new` UInt8 CODEC(LZ4HC)
)
ENGINE = MergeTree
ORDER BY date
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

--Step 9:
INSERT INTO prices_2
    SELECT price, date, postcode1, postcode2, is_new FROM uk_prices_3;

--Step 10:
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_2' AND active = 1
GROUP BY column;
