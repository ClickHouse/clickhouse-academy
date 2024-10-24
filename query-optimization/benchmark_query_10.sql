SELECT avg(fare_amount), avg(trip_distance) FROM taxi_zone_lookup JOIN $TABLE ON pickup_location_id = taxi_zone_lookup.id WHERE taxi_zone_lookup.zone ILIKE '%airport%'
