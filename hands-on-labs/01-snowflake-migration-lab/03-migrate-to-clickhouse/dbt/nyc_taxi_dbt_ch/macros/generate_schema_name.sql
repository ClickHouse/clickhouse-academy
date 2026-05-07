-- Override dbt's default schema naming so models with a custom schema
-- land directly in that schema rather than being prefixed with target.schema.
--
-- Example without this macro: target=nyc_taxi_ch + schema=analytics → "nyc_taxi_ch_analytics"
-- Example with this macro:    target=nyc_taxi_ch + schema=analytics → "analytics"
--
-- Why drop the prefix? In ClickHouse, schemas ARE databases. We want clean
-- database names (analytics, staging) not compound names (nyc_taxi_ch_analytics).
-- This matches how the migrated source data is organized.
--
-- Snowflake equivalent: generate_schema_name macro (identical pattern, different reason —
-- Snowflake uses it to avoid STAGING_ANALYTICS schema name prefixing).
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema | lower }}
  {%- else -%}
    {{ custom_schema_name | lower }}
  {%- endif -%}
{%- endmacro %}
