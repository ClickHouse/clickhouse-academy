--Step 1:
SELECT DISTINCT county
FROM uk_prices_3;

--Step 2:
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_prices_3
WHERE county = 'GREATER LONDON';

--Step 3:

ALTER TABLE uk_prices_3
    ADD INDEX county_index county
    TYPE bloom_filter
    GRANULARITY 1;

--Step 4:
ALTER TABLE uk_prices_3
    MATERIALIZE INDEX county_index;

--Step 5:
SELECT *
FROM system.mutations;

SELECT *
FROM system.mutations
WHERE table = 'uk_prices_3';

--Step 6:
SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

--Step 7:

/*
 * About 5.6M rows were scanned, instead of the entire 30M rows.
 */

--Step 9:
EXPLAIN indexes = 1 SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_prices_3
WHERE county = 'GREATER LONDON';
