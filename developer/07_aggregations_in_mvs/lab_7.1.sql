--Step 2:
CREATE TABLE prices_sum_dest
(
    town LowCardinality(String),
    sum_price UInt64
)
ENGINE = SummingMergeTree
PRIMARY KEY town;

CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest
AS
    SELECT
        town,
        sum(price) AS sum_price
    FROM uk_price_paid
    GROUP BY town;

INSERT INTO prices_sum_dest
    SELECT
        town,
        sum(price) AS sum_price
    FROM uk_price_paid
    GROUP BY town;

--Step 3:
SELECT count()
FROM prices_sum_dest;

--Step 4:

/*
 * The issue is that prices_sum_dest might have multiple rows with the same
 * primary key (e.g. LONDON). Therefore, you should always aggregate the rows
 * by using sum and the GROUP BY in the query.
 */
/* The fixed query looks like the following: */

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
GROUP BY town;

--Step 5:
SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
GROUP BY town
ORDER BY sum DESC
LIMIT 10;