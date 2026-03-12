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

INSERT INTO uk_prices_2
    SELECT * 
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

-- If you are using the Clickhouse OSS, run this query instead
INSERT INTO uk_prices_2
SELECT
    c1 as id,                 
    c2 as price,        
    parseDateTimeBestEffortOrNull(c3) AS date,
    c4 as postcode,     
    c5 as type,         
    c6 as is_new,      
    c7 as duration,    
    c8 as addr1,
    c9 as addr2,
    c10 as street,
    c11 as locality,
    c12 as town,
    c13 as district,
    c14 as county,
    c15 as column15,
    c16 as column16
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

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
