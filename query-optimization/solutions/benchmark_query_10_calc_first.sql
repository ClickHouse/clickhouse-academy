SELECT zone, avg_data.avg_fare_amount, avg_data.avg_trip_distance FROM (SELECT pickup_location_id, avg(fare_amount) AS avg_fare_amount, avg(trip_distance) AS avg_trip_distance FROM $TABLE GROUP BY pickup_location_id) AS avg_data JOIN taxi_zone_lookup ON avg_data.pickup_location_id = id WHERE zone ILIKE '%airport%'