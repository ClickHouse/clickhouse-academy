SELECT vendor_id, avg(total_amount), avg(trip_distance) FROM $TABLE GROUP BY vendor_id ORDER BY vendor_id DESC
