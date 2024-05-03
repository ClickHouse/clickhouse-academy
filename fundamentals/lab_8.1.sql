--Step 1:
SELECT * FROM crypto_prices 
WHERE crypto_name = 'Bitcoin';

--Step 2:
/*
 * The query processed 8,192 - so exactly 1 granule. The primary key of the
 * table is the crypto_name column, so filtering by crypto_name is the best
 * performance you will get.
 */

--Step 3:
SELECT * FROM crypto_prices
WHERE volume >= 400000000;

--Step 4:
/*
 * It scanned all 2.3M rows in the table (over 290 granules), because it had
 * to scan every granule. The volume column is not a part of the primary key,
 * so the primary key is no help when filtering by volume.
 */

--Step 5:
ALTER TABLE crypto_prices
ADD INDEX volume_index volume 
TYPE minmax
GRANULARITY 1;

--Step 6:
ALTER TABLE crypto_prices
MATERIALIZE INDEX volume_index;

--Step 7:
SELECT * FROM crypto_prices
WHERE volume >= 400000000;

/*
 * Notice this time less than 700,000 rows were processed (about 85 granules),
 * because the minmax skipping index of the volume column allowed ClickHouse
 * to skip over 200 granules that could not have contained a value of volume
 * more than 400000000.
 */