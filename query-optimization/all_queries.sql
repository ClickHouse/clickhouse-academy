-- query 1: What is the average trip time?
SELECT 
  avg(dateDiff('s', pickup_datetime,dropoff_datetime))
FROM 
  nyc_taxi;

-- query 2: What are the trip cost quantiles?
SELECT
  quantiles(0.5, 0.75, 0.9, 0.99)(total_amount)
FROM nyc_taxi;

-- query 3: What are the top 10 pickup zones?
SELECT taxi_zone_lookup.borough, taxi_zone_lookup.zone, count() 
FROM nyc_taxi AS taxi_rides 
JOIN taxi_zone_lookup 
ON taxi_rides.pickup_location_id = taxi_zone_lookup.id 
GROUP BY taxi_zone_lookup.borough, taxi_zone_lookup.zone 
ORDER BY 3 DESC 
LIMIT 10;

-- query 4: What is the sum of tolls paid?
SELECT sum(tolls_amount) 
FROM nyc_taxi;

-- query 5: What is the average price of trips longer than 5 miles?
SELECT avg(total_amount) 
FROM nyc_taxi 
WHERE trip_distance > 5;

-- query 6: What is the distance distribution in rides with avg(speed) > 100mph
WITH 
    dateDiff('s', pickup_datetime, dropoff_datetime) AS trip_time, 
    trip_distance::Decimal64(2) / trip_time * 3600 AS speed_mph 
SELECT 
    quantiles(0.5, 0.75, 0.9, 0.99)(trip_distance) 
FROM 
    nyc_taxi 
WHERE trip_time > 0 AND speed_mph > 100 

-- query 7: What are the average cost and distance per vendor?
SELECT
  vendor_id,
  avg(total_amount),
  avg(trip_distance),
FROM
  nyc_taxi
GROUP BY vendor_id
ORDER BY vendor_id DESC;

-- query 8: Quarter main summary (trips, distance, total, tips)
SELECT 
    payment_type,
    COUNT() AS trip_count,
    formatReadableQuantity(SUM(trip_distance)) AS total_distance,
    formatReadableQuantity(SUM(total_amount)) AS total_amount_sum,
    formatReadableQuantity(SUM(tip_amount)) AS tip_amount_sum
FROM 
    nyc_taxi
WHERE 
    pickup_datetime >= '2009-01-01' AND pickup_datetime < '2009-04-01'
GROUP BY 
    payment_type
ORDER BY 
    trip_count DESC;

-- query 9: Total number of trips per passenger_count?
SELECT
  passenger_count,
  count() as count,
  avg(fare_amount),
FROM
  nyc_taxi
GROUP BY passenger_count
ORDER BY count() DESC;

-- query 10: What are the average fare and distance of rides originating at airports?
SELECT 
    avg(fare_amount),
    avg(trip_distance) 
FROM 
    taxi_zone_lookup 
JOIN 
    nyc_taxi
ON 
    pickup_location_id = taxi_zone_lookup.id 
WHERE 
    taxi_zone_lookup.zone ILIKE '%airport%';


-- query 11: What is the weekly count of the number of rides and their average cost?
WITH
    toStartOfWeek(pickup_datetime) AS week
SELECT
    week,
    count(),
    avg(toDecimal32(fare_amount,2))
FROM nyc_taxi
GROUP BY week
ORDER BY week ASC;

-- query 12: The most expensive ride (including destination details) for each date
SELECT
    ride_date,
    max_fare,
    dropoff_id,
    taxi_zone_lookup.borough,
    taxi_zone_lookup.zone
FROM
    (
        SELECT
            toDate(pickup_datetime) AS ride_date,
            max(fare_amount) AS max_fare,
            argMax(dropoff_location_id, fare_amount) AS dropoff_id
        FROM
            nyc_taxi
        GROUP BY
            toDate(pickup_datetime)
    ) AS max_fares
JOIN
    taxi_zone_lookup
ON
    max_fares.dropoff_id = taxi_zone_lookup.id
ORDER BY
    ride_date ASC;
