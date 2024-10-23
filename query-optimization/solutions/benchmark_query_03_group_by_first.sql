SELECT taxi_zone_lookup.borough, taxi_zone_lookup.zone, pickup_counts.pickup_count FROM (SELECT pickup_location_id, count() AS pickup_count FROM $TABLE GROUP BY pickup_location_id
) AS pickup_counts
INNER JOIN taxi_zone_lookup AS taxi_zone_lookup ON pickup_counts.pickup_location_id = taxi_zone_lookup.id
ORDER BY pickup_counts.pickup_count DESC
LIMIT 10