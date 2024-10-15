SELECT avg(dateDiff('s', pickup_datetime, dropoff_datetime)) FROM nyc_taxi_inferred
SELECT quantiles(0.5, 0.75, 0.9, 0.99)(total_amount) FROM nyc_taxi_inferred
SELECT tzl.borough, tzl.zone, count() FROM nyc_taxi_inferred AS nyct JOIN taxi_zone_lookup_inferred AS tzl ON nyct.pickup_location_id = tzl.id GROUP BY tzl.borough, tzl.zone ORDER BY 3 DESC LIMIT 10
SELECT SUM(mta_tax) FROM nyc_taxi_inferred
SELECT avg(total_amount) FROM nyc_taxi_inferred WHERE trip_distance > 5
WITH dateDiff('s', pickup_datetime, dropoff_datetime) AS trip_time, trip_distance::Decimal64(2) / trip_time * 3600 AS speed_mph SELECT quantiles(0.5, 0.75, 0.9, 0.99)(trip_distance) FROM nyc_taxi_inferred WHERE speed_mph > 100 AND trip_time > 0
SELECT vendor_id, avg(total_amount), avg(trip_distance) FROM nyc_taxi_inferred GROUP BY vendor_id ORDER BY vendor_id DESC
SELECT payment_type, COUNT() AS trip_count, formatReadableQuantity(SUM(trip_distance)::Float64) AS total_distance, formatReadableQuantity(SUM(total_amount)::Float64) AS total_amount_sum, formatReadableQuantity(SUM(tip_amount)::Float64) AS tip_amount_sum FROM nyc_taxi_inferred WHERE pickup_datetime >= '2009-01-01' AND pickup_datetime < '2009-04-01' GROUP BY payment_type ORDER BY trip_count DESC
SELECT passenger_count, count(), avg(fare_amount) FROM nyc_taxi_inferred GROUP BY passenger_count ORDER BY count() DESC
SELECT avg(fare_amount), avg(trip_distance) FROM taxi_zone_lookup_inferred JOIN nyc_taxi_inferred ON pickup_location_id = taxi_zone_lookup_inferred.id WHERE taxi_zone_lookup_inferred.zone ILIKE '%airport%'
WITH toStartOfWeek(pickup_datetime) AS week SELECT week, count(), avg(toDecimal32(fare_amount,2)) FROM nyc_taxi_inferred GROUP BY week ORDER BY week ASC
SELECT ride_date, max_fare, dropoff_id, tzl.borough, tzl.zone FROM (SELECT toDate(pickup_datetime) AS ride_date, max(fare_amount) AS max_fare, argMax(dropoff_location_id, fare_amount) AS dropoff_id FROM nyc_taxi_inferred GROUP BY toDate(pickup_datetime)) AS max_fares JOIN taxi_zone_lookup_inferred AS tzl ON max_fares.dropoff_id = tzl.id ORDER BY ride_date ASC
