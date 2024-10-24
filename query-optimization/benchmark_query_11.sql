WITH toStartOfWeek(pickup_datetime) AS week SELECT week, count(), avg(toDecimal32(fare_amount,2)) FROM $TABLE GROUP BY week ORDER BY week ASC
