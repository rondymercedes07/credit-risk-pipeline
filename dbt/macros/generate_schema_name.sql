{#-
    Use the custom schema verbatim (staging / marts) instead of dbt's default
    "<target_schema>_<custom_schema>". Environments are separated by database
    (CREDIT_RISK_DEV / CREDIT_RISK_PROD / the DuckDB file), so schema names stay identical
    on every target and the same project runs unchanged on Snowflake and DuckDB.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
