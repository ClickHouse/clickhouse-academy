/* 
* Inserting an Imperfect CSV File
*/

-- Count rows in data file
SELECT count()
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';

-- Sum the actual_amount column
SELECT formatReadableQuantity(sum(actual_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';

-- Try to run a query that sums up the approved_amount column
SELECT formatReadableQuantity(sum(approved_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';

/*
 * You get an exception telling you that trying to sum a String column is not
 * allowed. Apparently, the approved_amount column is not entirely numeric
 * data, and ClickHouse inferred that column as a String.
 */

-- Use DESCRIBE to view the inferred schema. 
-- approved_amount and recommended_amount columns are inferred as Nullable(String)
DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';

-- sum up the values approved_amount and recommended_amount with toUInt32OrZero.
SELECT
    formatReadableQuantity(sum(toUInt32OrZero(approved_amount))),
    formatReadableQuantity(sum(toUInt32OrZero(recommended_amount)))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';

-- does using schema_inference_hints help?
SELECT
    formatReadableQuantity(sum(approved_amount)),
    formatReadableQuantity(sum(recommended_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS
format_csv_delimiter='~',
schema_inference_hints='approved_amount UInt32, recommended_amount UInt32';
-- It fails because the `approved_amount` contains 'n/a' which cannot be parsed to UInt32.

-- Create a table for the data with target data types
CREATE TABLE operating_budget (
    fiscal_year LowCardinality(String),
    service LowCardinality(String),
    department LowCardinality(String),
    program LowCardinality(String),
    program_code LowCardinality(String),
    description String,
    item_category LowCardinality(String),
    approved_amount UInt32,
    recommended_amount UInt32,
    actual_amount Decimal(12,2),
    fund LowCardinality(String),
    fund_type Enum8('GENERAL FUNDS' = 1, 'FEDERAL FUNDS' = 2, 'OTHER FUNDS' = 3)
)
ENGINE = MergeTree
PRIMARY KEY (fiscal_year, program);

-- Insert data from file to the new table
INSERT INTO operating_budget
    WITH
        splitByChar('(', c4) AS result
    SELECT
        c1 AS fiscal_year,
        c2 AS service,
        c3 AS department,
        result[1] AS program,
        splitByChar(')',result[2])[1] AS program_code,
        c5 AS description,
        c6 AS item_category,
        toUInt32OrZero(c7) AS approved_amount,
        toUInt32OrZero(c8) AS recommended_amount,
        toDecimal64(c9, 2) AS actual_amount,
        c10 AS fund,
        c11 AS fund_type
    FROM s3(
        'https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv',
        'CSV',
        'c1 String,
        c2 String,
        c3 String,
        c4 String,
        c5 String,
        c6 String,
        c7 String,
        c8 String,
        c9 String,
        c10 String,
        c11 String'
        )
    SETTINGS
        format_csv_delimiter = '~',
        input_format_csv_skip_first_lines=1;

--Select all rows from table
SELECT * FROM operating_budget;

--Sum the approved_amount column for fiscal year 2022
SELECT formatReadableQuantity(sum(approved_amount))
FROM operating_budget
WHERE fiscal_year = '2022';

--Sum the actual_amount column for 2022 and program_code 031
SELECT sum(actual_amount)
FROM operating_budget
WHERE fiscal_year = '2022'
AND program_code = '031';
