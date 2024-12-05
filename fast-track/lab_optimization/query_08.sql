SELECT toStartOfWeek(pickup_datetime) AS week, count(), avg(toDecimal32(fare_amount,2)) FROM nyc_taxi GROUP BY week ORDER BY week ASC
