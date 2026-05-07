{{
  config(
    materialized = 'table',
    schema       = 'ANALYTICS'
  )
}}

-- Payment type reference — seeded by scripts/01_create_tables.sql
SELECT payment_type_id, payment_code, payment_desc
FROM NYC_TAXI_DB.ANALYTICS.DIM_PAYMENT_TYPE
