{#-
    Staging casts RAW VARCHAR to typed columns. Every cast is a TRY cast: a malformed value
    becomes NULL instead of aborting the build, and is then caught by a test or reported in
    docs/data_quality_findings.md. The only syntax that differs per engine lives here, behind
    adapter.dispatch, so the same models run on Snowflake (dev/prod) and DuckDB (ci).
-#}

{% macro to_double(col) -%}
    {{ return(adapter.dispatch('to_double', 'credit_risk')(col)) }}
{%- endmacro %}

{% macro default__to_double(col) -%}
    try_cast({{ col }} as double)
{%- endmacro %}

{% macro snowflake__to_double(col) -%}
    try_to_double({{ col }})
{%- endmacro %}


{% macro to_decimal(col, precision=18, scale=4) -%}
    {{ return(adapter.dispatch('to_decimal', 'credit_risk')(col, precision, scale)) }}
{%- endmacro %}

{% macro default__to_decimal(col, precision, scale) -%}
    try_cast({{ col }} as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}

{% macro snowflake__to_decimal(col, precision, scale) -%}
    try_to_decimal({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}


{#- The Kaggle CSVs write integers as "12.0" in some columns, which neither engine casts
    straight to an integer type. Go through DOUBLE, then round. Fractional values in an
    integer column would be silently rounded, so the profiling step checks for them. -#}
{% macro to_bigint(col) -%}
    cast(round({{ to_double(col) }}, 0) as bigint)
{%- endmacro %}


{#- 0/1 flags -> boolean; any other value becomes NULL (and is surfaced by profiling). -#}
{% macro to_bool_01(col) -%}
    case {{ to_bigint(col) }} when 1 then true when 0 then false end
{%- endmacro %}

{#- Y/N flags -> boolean. -#}
{% macro to_bool_yn(col) -%}
    case {{ col }} when 'Y' then true when 'N' then false end
{%- endmacro %}

{% macro clean_text(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{#- "XNA" is Home Credit's "not available" code. Normalised to NULL; the is_<col>_xna flag
    that goes with it keeps the original fact. -#}
{% macro null_if_xna(col) -%}
    case when trim({{ col }}) = 'XNA' then null else {{ clean_text(col) }} end
{%- endmacro %}

{% macro is_xna(col) -%}
    coalesce(trim({{ col }}) = 'XNA', false)
{%- endmacro %}


{#- 365243 (about 1000 years) is a placeholder, not a duration. -#}
{% macro null_if_sentinel(col, sentinel=365243) -%}
    case when {{ to_bigint(col) }} = {{ sentinel }} then null else {{ to_bigint(col) }} end
{%- endmacro %}

{% macro is_sentinel(col, sentinel=365243) -%}
    coalesce({{ to_bigint(col) }} = {{ sentinel }}, false)
{%- endmacro %}
