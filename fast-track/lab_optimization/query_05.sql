SELECT passenger_count, count(), avg(fare_amount) FROM nyc_taxi GROUP BY passenger_count ORDER BY count() DESC
