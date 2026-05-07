-- 04_create_dictionary.sql
-- Creates a ClickHouse dictionary for fast zone lookup via dictGet().
-- Run after dbt completes (dim_taxi_zones must exist and be populated).
-- Called by setup.sh after Step 5.
--
-- Usage in SQL:
--   dictGet('analytics.taxi_zones_dict', 'borough',      toUInt16(pickup_location_id))
--   dictGet('analytics.taxi_zones_dict', 'zone',         toUInt16(pickup_location_id))
--   dictGet('analytics.taxi_zones_dict', 'service_zone', toUInt16(pickup_location_id))

CREATE OR REPLACE DICTIONARY analytics.taxi_zones_dict
(
    location_id  UInt16,
    zone         String,
    borough      String,
    service_zone String
)
PRIMARY KEY location_id
SOURCE(CLICKHOUSE(
    TABLE    'dim_taxi_zones'
    DB       'analytics'
    USER     'default'
    PASSWORD ''  -- override at runtime with actual password
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);
