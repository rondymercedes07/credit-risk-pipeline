with source as (

    select * from {{ source('raw', 'bureau') }}

),

typed as (

    select
        {{ to_bigint('sk_id_curr') }} as sk_id_curr,
        {{ to_bigint('sk_id_bureau') }} as sk_id_bureau,
        {{ clean_text('credit_active') }} as credit_active,
        {{ clean_text('credit_currency') }} as credit_currency,
        {{ to_bigint('days_credit') }} as days_credit,
        {{ to_bigint('credit_day_overdue') }} as credit_day_overdue,
        {{ to_bigint('days_credit_enddate') }} as days_credit_enddate,
        {{ to_bigint('days_enddate_fact') }} as days_enddate_fact,
        {{ to_decimal('amt_credit_max_overdue') }} as amt_credit_max_overdue,
        {{ to_bigint('cnt_credit_prolong') }} as cnt_credit_prolong,
        {{ to_decimal('amt_credit_sum') }} as amt_credit_sum,
        {{ to_decimal('amt_credit_sum_debt') }} as amt_credit_sum_debt,
        {{ to_decimal('amt_credit_sum_limit') }} as amt_credit_sum_limit,
        {{ to_decimal('amt_credit_sum_overdue') }} as amt_credit_sum_overdue,
        {{ clean_text('credit_type') }} as credit_type,
        {{ to_bigint('days_credit_update') }} as days_credit_update,
        {{ to_decimal('amt_annuity') }} as amt_annuity,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
