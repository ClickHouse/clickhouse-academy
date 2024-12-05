SELECT avg(fare_amount), avg(trip_distance) FROM nyc_taxi WHERE dictGet('taxi_zone_lookup','zone',pickup_location_id) ILIKE '%airport%'

