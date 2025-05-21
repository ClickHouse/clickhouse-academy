-- Step 1
SELECT 
    id,
    replaceRegexpAll(id,'[{}]','')
FROM uk_prices_2
LIMIT 100;

-- Step 2
WITH
    splitByChar(' ', postcode) AS postcodes
SELECT
    postcodes[1] AS postcode1,
    postcodes[2] AS postcode2
FROM uk_prices_2
WHERE postcode != ''
LIMIT 100;

-- Step 3
SELECT
    uniq(postcode1),
    uniq(postcode2)
FROM (
    WITH
    splitByChar(' ', postcode) AS postcodes
    SELECT
        postcodes[1] AS postcode1,
        postcodes[2] AS postcode2
    FROM uk_prices_2
    WHERE postcode != ''
);

-- Step 4
SELECT 
    uniq(postcode),
    uniq(addr1),
    uniq(addr2),
    uniq(street),
    uniq(locality),
    uniq(town),
    uniq(district),
    uniq(county) 
FROM uk_prices_2;

-- Step 6
CREATE TABLE uk_prices_3
(
    id UUID,
    price UInt32,
    date DateTime,
    postcode1 String,
    postcode2 String,
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street String,
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (postcode1, postcode2);

-- Step 7
INSERT INTO uk_prices_3
    WITH
        splitByChar(' ', postcode) AS postcodes
    SELECT
        replaceRegexpAll(id,'[{}]','') AS id,
        toUInt32(price) AS price,
        date,
        postcodes[1] AS postcode1,
        postcodes[2] AS postcode2,
        transform(type, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other'],'other') AS type,
        is_new = 'Y' AS is_new,
        transform(duration, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown'],'unknown') AS duration,
        addr1,
        addr2,
        street,
        locality,
        town,
        district,
        county
    FROM uk_prices_2;

-- Step 8
SELECT count() FROM uk_prices_3;

-- Step 9
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table ilike 'uk_prices_%' AND active = 1
GROUP BY table
ORDER BY table;

-- Step 10
SELECT 
    town,
    max(price)
FROM uk_prices_3
GROUP BY town 
ORDER BY 2 DESC
LIMIT 25;

-- Step 11
SELECT
    avg(price),
    max(price) 
FROM uk_prices_3 
WHERE postcode1 = 'BD16'
AND postcode2 = '1AE';

-- Step 12
SELECT
    avg(price),
    max(price) 
FROM uk_prices_3 
WHERE postcode2 = '1AE';
