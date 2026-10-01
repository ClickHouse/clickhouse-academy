SELECT 
   dictGet('taxi_zone_lookup_dict','borough',pickup_location_id) AS borough,
   dictGet('taxi_zone_lookup_dict','zone',pickup_location_id) AS zone,
   pickup_count
FROM 
    (
        SELECT pickup_location_id, count() AS pickup_count
        FROM $TABLE
        GROUP BY pickup_location_id
    )
ORDER BY pickup_count DESC
LIMIT 10
