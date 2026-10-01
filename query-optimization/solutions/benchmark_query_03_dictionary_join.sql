SELECT 
   taxi_zone_lookup_dict.borough,
   taxi_zone_lookup_dict.zone,
   count() AS pickup_count
FROM $TABLE AS taxi_rides
JOIN taxi_zone_lookup_dict
ON taxi_rides.pickup_location_id = taxi_zone_lookup_dict.id
GROUP BY taxi_zone_lookup_dict.borough, taxi_zone_lookup_dict.zone
ORDER BY pickup_count DESC
LIMIT 10
