--Step 1:
SELECT
    count(),
    avg(price)
FROM uk_prices_3
WHERE toYear(date) = '2020';

--Step 2:
WITH
    toYear(date) AS year
SELECT
    year,
    count(),
    avg(price)
FROM uk_prices_3
GROUP BY year
ORDER BY year ASC;

--Step 3:
CREATE TABLE prices_by_year_dest (
    price UInt32,
    date Date,
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date);

--Step 4:
CREATE MATERIALIZED VIEW prices_by_year_view
TO prices_by_year_dest
AS
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_prices_3;

--Step 5:
INSERT INTO prices_by_year_dest
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_prices_3;

--Step 6:
SELECT count()
FROM prices_by_year_dest;

--Step 7:
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

--Step 8:
SELECT * FROM system.parts
WHERE table='uk_prices_3';

--Step 10:
SELECT
    count(),
    avg(price)
FROM prices_by_year_dest
WHERE toYear(date) = '2020';

/*
 * The query only needs to read 886,642 rows, which is exactly how many
 * properties were sold in the UK in 2020.
 */

--Step 11:
SELECT
    count(),
    max(price),
    avg(price),
    quantile(0.90)(price)
FROM prices_by_year_dest
WHERE county = 'STAFFORDSHIRE'
    AND date >= toDate('2005-06-01') AND date <= toDate('2005-06-30');

--Step 12:
INSERT INTO uk_prices_3 VALUES
    ('51f279f5-ef5f-46e1-bd8e-b6c4159d8fa7', 125000, '1994-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    ('a0d2f609-b6f9-4972-857c-8e4266d146ae', 440000000, '1994-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    ('1017aff1-6f1e-420a-aad5-7d03ce60c8c5', 2000000, '1994-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

--Step 13:
SELECT * FROM prices_by_year_dest
WHERE toYear(date) = '1994';

--Step 14:
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';