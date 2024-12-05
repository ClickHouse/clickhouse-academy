CREATE OR REPLACE TABLE employees ( 
    emp_id UInt16,
    start Date,
    salary UInt64,
    level LowCardinality(String),
    PROJECTION level_projection
    (
        SELECT level,salary
        ORDER BY level
    )
)
PRIMARY KEY start;

insert into employees
VALUES 
 (1001, '2010-12-12',274020,'principle'),
 (1002, '2021-04-23',65000,'associate'),
 (1003, '2022-05-21',110000,'senior'),
 (1004, '2024-01-04',80500,'associate');
 
SELECT count()
FROM employees
WHERE start < '2020-04-15';

SELECT count()
FROM employees
WHERE start < '2020-04-15';
