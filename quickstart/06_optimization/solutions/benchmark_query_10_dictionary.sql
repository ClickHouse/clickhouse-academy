SELECT 
   avg(fare_amount), 
   avg(trip_distance) 
FROM $TABLE
WHERE dictGet('taxi_zone_lookup','zone',pickup_location_id) ILIKE '%airport%'
