SELECT passenger_count, count() AS trip_count, avg(fare_amount) FROM nyc_taxi GROUP BY passenger_count ORDER BY trip_count DESC
