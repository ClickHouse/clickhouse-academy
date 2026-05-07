{{
  config(
    materialized = 'table',
    engine       = 'MergeTree()',
    order_by     = '(payment_type_id)',
    schema       = 'analytics'
  )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- dim_payment_type — Payment type lookup (static seed data)
--
-- Migration note: In Snowflake Part 1, payment types were inserted via the
-- 01_create_tables.sql seed script:
--   INSERT INTO DIM_PAYMENT_TYPE VALUES (1, 'CREDIT', 'Credit Card'), ...
-- and dim_payment_type.sql referenced NYC_TAXI_DB.ANALYTICS.DIM_PAYMENT_TYPE.
--
-- In ClickHouse, the Python migration script only migrates trip data; zone data is
-- seeded separately. Payment type is static reference data that doesn't exist in
-- any raw table, so we embed the VALUES directly in the model. This creates a
-- self-contained dbt project with no external seed dependencies for this dimension.
--
-- SQL translation:
--   INSERT INTO ... VALUES  →  SELECT * FROM (VALUES ...) as inline CTE
--   No Snowflake-specific syntax — VALUES is ANSI SQL, supported in ClickHouse.
-- ════════════════════════════════════════════════════════════════════════════

SELECT *
FROM (
    SELECT 1 AS payment_type_id, 'CREDIT'  AS payment_code, 'Credit Card'  AS payment_desc
    UNION ALL SELECT 2, 'CASH',    'Cash'
    UNION ALL SELECT 3, 'NO_CHG',  'No Charge'
    UNION ALL SELECT 4, 'DISPUTE', 'Dispute'
    UNION ALL SELECT 5, 'UNKNOWN', 'Unknown'
    UNION ALL SELECT 6, 'VOIDED',  'Voided Trip'
)
