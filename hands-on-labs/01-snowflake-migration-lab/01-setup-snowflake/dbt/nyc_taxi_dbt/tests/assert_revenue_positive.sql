-- Custom test: all fare amounts must be non-negative
-- A passing test returns 0 rows
SELECT
    trip_id,
    total_amount_usd,
    fare_amount_usd
FROM {{ ref('fact_trips') }}
WHERE total_amount_usd < 0
   OR fare_amount_usd < 0
