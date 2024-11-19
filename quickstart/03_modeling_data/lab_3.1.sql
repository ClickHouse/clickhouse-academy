--Step 1:
DESCRIBE pypi;

--Step 2:
SELECT uniqExact(COUNTRY_CODE)
FROM pypi;

/*
 * You will notice there are only 186 unique values of the country code, which
 * makes it a great candidate for LowCardinality.
 */

--Step 3:
SELECT
    uniqExact(PROJECT),
    uniqExact(URL)
FROM pypi;

/*
 * There are over 24,000 unique values of PROJECT, which is large - but not too
 * large. We will try LowCardinality on this column as well and see if it
 * improves storage and query performance. The URL has over 79,000 unique
 * values, and we can assume that a URL could have a lot of different values,
 * so it is probably a bad choice for LowCardinality.
 */

--Step 4:
CREATE TABLE pypi3 (
    TIMESTAMP DateTime,
    COUNTRY_CODE LowCardinality(String),
    URL String,
    PROJECT LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi3
    SELECT * FROM pypi2;

--Step 5:
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%')
GROUP BY table;

--Step 6:
SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi2
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;