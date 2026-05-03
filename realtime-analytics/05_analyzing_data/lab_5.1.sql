--Step 1:
SELECT *
FROM uk_prices_3
WHERE price >= 100_000_000
ORDER BY price desc;

--Step 2:
SELECT count()
FROM uk_prices_3
WHERE
    price > 1_000_000
    AND toYear(date) = '2024';

--Step 3:
SELECT uniqExact(town)
FROM uk_prices_3;

--Step 4:
SELECT
    town,
    count() AS c
FROM uk_prices_3
GROUP BY town
ORDER BY c DESC
LIMIT 1;

--Step 5:
SELECT topKIf(10)(town, town != 'LONDON')
FROM uk_prices_3;

--Step 6:
SELECT
    town,
    avg(price) AS avg_price
FROM uk_prices_3
GROUP BY town
ORDER BY avg_price DESC
LIMIT 10;

--Step 7:
SELECT
    addr1,
    addr2,
    street,
    town
FROM uk_prices_3
ORDER BY price DESC
LIMIT 1;

--Step 8:
SELECT
    avgIf(price, type = 'detached'),
    avgIf(price, type = 'semi-detached'),
    avgIf(price, type = 'terraced'),
    avgIf(price, type = 'flat'),
    avgIf(price, type = 'other')
FROM uk_prices_3;

SELECT type, avg(price) as avg_price
FROM uk_prices_3
GROUP BY type;

--Step 9:
SELECT
    sum(price)
FROM uk_prices_3
WHERE
    county IN ['AVON','ESSEX','DEVON','KENT','CORNWALL']
    AND
    toYear(date) = '2024';


--Step 10:
SELECT
    toStartOfMonth(date) AS month,
    avg(price) AS avg_price
FROM uk_prices_3
WHERE
    toYear(month) BETWEEN '2005' AND '2010'
GROUP BY month
ORDER BY month ASC;

--Step 11:
SELECT
    toStartOfDay(date) AS day,
    count()
FROM uk_prices_3
WHERE
    town = 'LIVERPOOL'
    AND toYear(date) = 2020
GROUP BY day
ORDER BY day ASC;

--Step 12:
WITH (
    SELECT max(price)
    FROM uk_prices_3
) AS overall_max
SELECT
    town,
    max(price) / overall_max
FROM uk_prices_3
GROUP BY town
ORDER BY 2 DESC;
