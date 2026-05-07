{{
  config(
    materialized = 'table',
    engine       = 'MergeTree()',
    order_by     = '(vendor_id)',
    schema       = 'analytics'
  )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- dim_vendor — Vendor lookup (static seed data)
--
-- Migration note: Same pattern as dim_payment_type. In Snowflake Part 1, vendor
-- data was seeded via 01_create_tables.sql INSERT statements and referenced as
-- NYC_TAXI_DB.ANALYTICS.DIM_VENDOR.
--
-- In ClickHouse, vendor data is embedded directly in the model as static VALUES.
-- This avoids any external seed file dependency for small static dimensions.
--
-- These are the three TLC-registered taxi vendors in the NYC Taxi dataset:
--   CMT = Creative Mobile Technologies (app/dispatch system)
--   VTS = VeriFone Inc. (payment terminals)
--   DDS = Digital Dispatch Systems
-- ════════════════════════════════════════════════════════════════════════════

SELECT *
FROM (
    SELECT 1 AS vendor_id, 'CMT' AS vendor_code, 'Creative Mobile Technologies' AS vendor_name
    UNION ALL SELECT 2, 'VTS', 'VeriFone Inc.'
    UNION ALL SELECT 3, 'DDS', 'Digital Dispatch Systems'
)
