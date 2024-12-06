-- USING TABLE FUNCTIONS --

-- Dataset URL:
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet

-- See the inferred schema of the s3 parquet file
DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

-- Count the number of lines 
SELECT count() 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

-- View a single line in vertical format
SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet') 
LIMIT 1 
FORMAT VERTICAL;

-- Count the number of downloads from the US
SELECT count() 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet') 
WHERE COUNTRY_CODE='US';

-- Create a table for dataset 
CREATE TABLE pypi (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY TIMESTAMP;

-- Insert the dataset into the table
INSERT INTO pypi
SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

-- Count the rows in the table
SELECT count()
FROM pypi;

-- Count downloads from the US
SELECT count() 
FROM pypi
WHERE COUNTRY_CODE='US';
