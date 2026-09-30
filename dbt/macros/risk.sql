{#-
    Business-logic helpers shared by the intermediate and mart layers. Only plain functions that
    behave identically on Snowflake and DuckDB are used, so no adapter dispatch is needed here.
    Day columns are days relative to each client's current application (negative = past).
-#}

{#- Delinquency bucket of a days-past-due value: 0 (on time or early), 1-30, 31-60, 61-90, 90+.
    NULL stays NULL (an unpaid installment has no known DPD). -#}
{% macro dpd_bucket(col) -%}
    case
        when {{ col }} is null then null
        when {{ col }} <= 0 then '0'
        when {{ col }} <= 30 then '1-30'
        when {{ col }} <= 60 then '31-60'
        when {{ col }} <= 90 then '61-90'
        else '90+'
    end
{%- endmacro %}

{#- Whole months elapsed before the current application, from a negative day count. -#}
{% macro months_before_application(days_col) -%}
    cast(floor(-{{ days_col }} / 30.4375) as bigint)
{%- endmacro %}

{#- 12-month block label for a month count: 0 -> '00-11m', 14 -> '12-23m'. The window is 96
    months, so 96 and above (a handful of clients) share one open block, '96m+'. -#}
{% macro cohort_block(months_col) -%}
    case
        when {{ months_col }} >= 96 then '96m+'
        else
            lpad(cast(cast(floor({{ months_col }} / 12) * 12 as bigint) as varchar), 2, '0')
            || '-' || lpad(cast(cast(floor({{ months_col }} / 12) * 12 + 11 as bigint) as varchar), 2, '0') || 'm'
    end
{%- endmacro %}
