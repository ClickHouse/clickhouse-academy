--Step 1:
DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

--Step 2:
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
LIMIT 10;

--Step 3:
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

--Step 4:
CREATE TABLE pypi (
    TIMESTAMP DateTime64,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY TIMESTAMP;

--Step 5:
INSERT INTO pypi
    SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

--Step 6:
SELECT
    PROJECT,
    count() AS c
FROM pypi
GROUP BY PROJECT
ORDER BY c DESC
LIMIT 100;

--Step 7:
/*
 * All of the rows were read, because the query had no WHERE clause - so
 * ClickHouse needed to process every granule.
 */

--Step 8:
SELECT
    PROJECT,
    count() AS c
FROM pypi
WHERE toStartOfMonth(TIMESTAMP) = '2023-04-01'
GROUP BY PROJECT
ORDER BY c DESC
LIMIT 100;

SELECT
    PROJECT,
    count() AS c
FROM pypi
WHERE TIMESTAMP >= toDate('2023-04-01') AND TIMESTAMP < toDate('2023-05-01')
GROUP BY PROJECT
ORDER BY c DESC
LIMIT 100;

--Step 9:
/*
 * Your answer may vary by a granule or two, but the query only has to process
 * 565,248 rows, which is exactly 8,192 x 69. So the query processed 69
 * granules instead of performing a scan of the entire table. Why? Because the
 * primary key is the TIMESTAMP column, which allows ClickHouse to skip about
 * 1/3 of the data.
*/

--Step 10:
SELECT
    PROJECT,
    count() AS c
FROM pypi
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

--Step 11:
/*
 * The PROJECT column is not in the primary key, so the primary index is no
 * help in skipping granules.
 */

--Step 12:
CREATE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP, PROJECT);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

--Step 13:
/*
 * None. Even though PROJECT was added to the primary key, it did not allow
 * ClickHouse to skip any granules. Why? Because the TIMESTAMP has a high
 * cardinality that is making any subsequent columns in the primary key
 * difficult to be useful.
 */


--Step 14:
CREATE OR REPLACE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

--Step 15:
/*
 * The first column of the primary key is an important and powerful design
 * decision. By putting PROJECT first, we are assuring that our queries that
 * filter by PROJECT will process a minimum amount of rows.
 */