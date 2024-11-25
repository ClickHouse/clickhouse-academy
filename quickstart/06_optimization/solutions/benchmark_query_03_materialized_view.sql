SELECT tzl.borough, tzl.zone, countMerge(number_of_trips) FROM $TABLE AS nyct JOIN taxi_zone_lookup AS tzl ON nyct.pickup_location_id = tzl.id GROUP BY tzl.borough, tzl.zone ORDER BY 3 DESC LIMIT 10
