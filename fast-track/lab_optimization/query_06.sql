SELECT avg(fare_amount), avg(trip_distance) FROM nyc_taxi JOIN taxi_zone_lookup ON pickup_location_id = taxi_zone_lookup.id WHERE taxi_zone_lookup.zone ILIKE '%airport%'
