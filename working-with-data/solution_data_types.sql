-- Create a test table to compare nullable and non-nullable columns
CREATE or replace TABLE null_test
(
    c1 String,
    c2 Int16,
    c3_nullable Nullable(Int16)
)
ORDER BY c1;

-- Insert a few rows with values for all columns and one row with missing values
INSERT INTO null_test VALUES ('A',0, 0),('B',10, 10),('C',20,20);
INSERT INTO null_test(c1) VALUES ('D');

-- Observe how aggregation functions exclude nulls
SELECT avg(c2),avg(c3_nullable) FROM null_test;
SELECT count(c2),count(c3_nullable) FROM null_test;

-- View the hidden null column
SELECT c3_nullable.null FROM null_test;

-- Create a copy of the pypi table that uses Nullable columns
CREATE or replace TABLE pypi_nullable
(
    TIMESTAMP DateTime,
    COUNTRY_CODE Nullable(String),
    URL Nullable(String),
    PROJECT Nullable(String)
)
PRIMARY KEY TIMESTAMP;

INSERT INTO pypi_nullable SELECT * FROM pypi;

-- Add up the size of the data of the parts in both tables and compare
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE (active = 1) AND (database = currentDatabase()) AND (table IN ('pypi','pypi_nullable'))
GROUP BY table;

SELECT uniq(TIMESTAMP),uniq(COUNTRY_CODE),uniq(URL),uniq(PROJECT) FROM pypi;

-- Create a version of the pypi table that uses LowCardinality columns
-- Create the table
CREATE or replace TABLE pypi_lowcard
(
    TIMESTAMP DateTime,
    COUNTRY_CODE LowCardinality(String),
    URL String,
    PROJECT String
)
PRIMARY KEY TIMESTAMP;

-- Insert the data
INSERT INTO pypi_lowcard SELECT * FROM pypi;

-- Compare the compressed and uncompressed sizes of both tables
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE (active = 1) AND (database = currentDatabase()) AND (table IN ('pypi','pypi_lowcard'))
GROUP BY table;