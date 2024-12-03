-- Show schema
SHOW CREATE TABLE pypi;

-- how many unique values are in COUNTRY_CODE
SELECT uniqExact(COUNTRY_CODE)
FROM pypi;

/*
 * You will notice there are only 186 unique values of the country code, which
 * makes it a great candidate for LowCardinality.
 */

SELECT
    uniqExact(PROJECT),
    uniqExact(URL)
FROM pypi;

/*
 * There are over 24,000 unique values of PROJECT, which is large - but not too
 * large. Try LowCardinality on this column as well and see if it
 * improves storage and query performance. The URL has over 79,000 unique
 * values, and you can assume that a URL could have a lot of different values,
 * so it is probably a bad choice for LowCardinality.
 */

-- Create a version of pypi using LowCardinality columns
CREATE TABLE pypi_low_cardinality (
    TIMESTAMP DateTime,
    COUNTRY_CODE LowCardinality(String),
    URL String,
    PROJECT LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

-- Populate the table with the pypi data
INSERT INTO pypi_low_cardinality
    SELECT * FROM pypi;

--View the sizes and number of parts of all pypi* tables
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%') AND (database = currentDatabase())
GROUP BY table;

--Step 6:
SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi_low_cardinality
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;