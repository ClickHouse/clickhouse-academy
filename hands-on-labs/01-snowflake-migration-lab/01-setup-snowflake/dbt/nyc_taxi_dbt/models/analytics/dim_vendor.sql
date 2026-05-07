{{
  config(
    materialized = 'table',
    schema       = 'ANALYTICS'
  )
}}

SELECT vendor_id, vendor_code, vendor_name
FROM NYC_TAXI_DB.ANALYTICS.DIM_VENDOR
