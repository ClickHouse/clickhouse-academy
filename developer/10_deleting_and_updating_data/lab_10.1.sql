--Step 1:
CREATE TABLE rates_monthly (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY month;

--Step 2:
-- If you are using ClickHouse OSS, use `parseDateTime(date, '%d/%m/%Y')` instead of `toDate(date)`.
INSERT INTO rates_monthly
    SELECT
        toDate(date) AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');

--Step 3:
SELECT *
FROM rates_monthly;

--Step 4:
SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

--Step 5:
INSERT INTO rates_monthly VALUES
    ('2022-05-31', 3.2, 3.0, 1.1);

--Step 6:
SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

--Step 7:
SELECT *
FROM rates_monthly FINAL
WHERE month = '2022-05-31';

--Step 8:
CREATE TABLE rates_monthly2 (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2),
    version UInt32
)
ENGINE = ReplacingMergeTree(version)
PRIMARY KEY month;

--Step 9:
INSERT INTO rates_monthly2
    SELECT
        month, variable, fixed, bank, 1
    FROM rates_monthly;

--Step 10:
INSERT INTO rates_monthly2 VALUES
    ('2022-04-30', 3.1, 2.6, 1.1, 10);

INSERT INTO rates_monthly2 VALUES
    ('2022-04-30', 2.9, 2.4, 0.9, 5);

--Step 11:
SELECT *
FROM rates_monthly2 FINAL
WHERE month = '2022-04-30';

--Step 12:
OPTIMIZE TABLE rates_monthly2 FINAL;

SELECT *
FROM rates_monthly2
WHERE month = '2022-04-30';
