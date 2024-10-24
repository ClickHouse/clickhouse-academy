SELECT passenger_count, count(), avg(fare_amount) FROM $TABLE GROUP BY passenger_count ORDER BY count() DESC
