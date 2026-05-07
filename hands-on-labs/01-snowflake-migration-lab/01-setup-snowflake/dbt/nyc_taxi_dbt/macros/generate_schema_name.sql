-- Override dbt's default schema naming so that models with a custom schema
-- (e.g. +schema: ANALYTICS) land directly in that schema rather than being
-- prefixed with the target schema (e.g. STAGING_ANALYTICS).
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema | upper }}
  {%- else -%}
    {{ custom_schema_name | upper }}
  {%- endif -%}
{%- endmacro %}
