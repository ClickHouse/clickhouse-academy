{% macro generate_surrogate_key(field_list) %}
    -- Wrapper around dbt_utils.generate_surrogate_key for consistent SK generation
    -- Used in models that need a stable surrogate key from natural keys
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
