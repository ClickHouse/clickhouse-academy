-- Custom test: no future-dated pickups
-- A passing test returns 0 rows
SELECT
    trip_id,
    pickup_at,
    CURRENT_TIMESTAMP() AS now
FROM {{ ref('fact_trips') }}
WHERE pickup_at > CURRENT_TIMESTAMP()
