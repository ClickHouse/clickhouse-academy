-- Solutions for ReplacingMergeTree lab

-- Create a ReplacingMergeTree table with mortgage rates
CREATE OR REPLACE TABLE rates_monthly (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY month;

-- Insert data from CSV in S3
INSERT INTO rates_monthly
SELECT
    toDate(date) AS month,
    variable,
    fixed,
    bank
FROM s3(
    'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
    'CSVWithNames');

--Select all rows
SELECT *
FROM rates_monthly;

--Select rows from May 2022
SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

--Add a new row for May 2022
INSERT INTO rates_monthly VALUES
    ('2022-05-31', 3.2, 3.0, 1.1);

--Select rows from May 2022 again
SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

--Select only the most recently-added row from May 2022
SELECT *
FROM rates_monthly FINAL
WHERE month = '2022-05-31';

-- Create a ReplacingMergeTree with a versioning column
CREATE OR REPLACE TABLE rates_monthly_versioned (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2),
    version UInt32
)
ENGINE = ReplacingMergeTree(version)
PRIMARY KEY month;

-- Copy data from original, with version column = 1
INSERT INTO rates_monthly_versioned
    SELECT
        month, variable, fixed, bank, 1
    FROM rates_monthly;

-- View the initial data for April 2022
SELECT *
FROM rates_monthly_versioned 
WHERE month = '2022-04-30';

-- first update for April 2022
INSERT INTO rates_monthly_versioned VALUES
    ('2022-04-30', 3.1, 2.6, 1.1, 10);

-- second update for April 2022
INSERT INTO rates_monthly_versioned VALUES
    ('2022-04-30', 2.9, 2.4, 0.9, 5);

-- Select all the rows for April 2022
SELECT *
FROM rates_monthly_versioned 
WHERE month = '2022-04-30';

-- Select the April 2022 row with the highest version
SELECT *
FROM rates_monthly_versioned FINAL
WHERE month = '2022-04-30';
