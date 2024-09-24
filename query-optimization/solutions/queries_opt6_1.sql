SELECT avg(dateDiff('s', pickup_datetime, dropoff_datetime)) FROM $TABLE
SELECT quantiles(0.5, 0.75, 0.9, 0.99)(total_amount) FROM $TABLE
SELECT tzl.borough, tzl.zone, count() FROM $TABLE AS nyct JOIN taxi_zone_lookup_opt6_1 AS tzl ON nyct.pickup_location_id = tzl.id GROUP BY tzl.borough, tzl.zone ORDER BY 3 DESC LIMIT 10
SELECT SUM(mta_tax) FROM $TABLE
SELECT avg(total_amount) FROM $TABLE WHERE trip_distance > 5
WITH dateDiff('s', pickup_datetime, dropoff_datetime) AS trip_time, trip_distance::Decimal64(2) / trip_time * 3600 AS speed_mph SELECT quantiles(0.5, 0.75, 0.9, 0.99)(trip_distance) FROM $TABLE WHERE speed_mph > 100 AND trip_time > 0
SELECT vendor_id, avg(total_amount), avg(trip_distance) FROM $TABLE GROUP BY vendor_id ORDER BY vendor_id DESC
SELECT payment_type, COUNT() AS trip_count, formatReadableQuantity(SUM(trip_distance)::Float64) AS total_distance, formatReadableQuantity(SUM(total_amount)::Float64) AS total_amount_sum, formatReadableQuantity(SUM(tip_amount)::Float64) AS tip_amount_sum FROM $TABLE WHERE pickup_datetime >= '2009-10-01' AND pickup_datetime < '2010-01-01' GROUP BY payment_type ORDER BY trip_count DESC
SELECT passenger_count, count(), avg(fare_amount) FROM $TABLE GROUP BY passenger_count ORDER BY count() DESC
SELECT avg(fare_amount), avg(trip_distance) FROM taxi_zone_lookup_opt6_1 JOIN $TABLE ON pickup_location_id = taxi_zone_lookup_opt6_1.id WHERE taxi_zone_lookup_opt6_1.zone ILIKE '%airport%'
WITH toStartOfWeek(pickup_datetime) AS week SELECT week, count(), avg(toDecimal32(fare_amount,2)) FROM $TABLE GROUP BY week ORDER BY week ASC
SELECT day, max_fare, dropoff_id, tzl.borough, tzl.zone FROM (SELECT toDate(pickup_datetime) AS day, max(fare_amount) AS max_fare, argMax(dropoff_location_id, fare_amount) AS dropoff_id FROM $TABLE GROUP BY toDate(pickup_datetime)) AS max_fares JOIN taxi_zone_lookup_opt6_1 AS tzl ON max_fares.dropoff_id = tzl.id ORDER BY day ASC
