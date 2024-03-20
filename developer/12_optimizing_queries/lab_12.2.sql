--Step 3:
ALTER TABLE uk_price_paid
    ADD PROJECTION town_date_projection (
        SELECT
            town, date, price
        ORDER BY town,date
    );

--Step 4:
ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION town_date_projection;

--Step 5:

/*
 * The query only had to read 311,296 rows, 38 granules. (You may get a
 * slightly different result.) This is a great improvement over having to read
 * all 28M rows.
 */

--Step 6:

/*
 * The disk storage is now around 263MB, so your projection is using
 * about 263MB - 190MB = 73MB.
 */

--Step 7:
ALTER TABLE uk_price_paid
    ADD PROJECTION handy_aggs_projection (
        SELECT
            avg(price),
            max(price),
            sum(price)
        GROUP BY town
    );

--Step 8:
ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION handy_aggs_projection;

--Step 11:
EXPLAIN SELECT
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL';