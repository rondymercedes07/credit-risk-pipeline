with source as (

    select * from {{ source('raw', 'credit_card_balance') }}

),

typed as (

    select
        {{ to_bigint('sk_id_prev') }} as sk_id_prev,
        {{ to_bigint('sk_id_curr') }} as sk_id_curr,
        {{ to_bigint('months_balance') }} as months_balance,
        {{ to_decimal('amt_balance') }} as amt_balance,
        {{ to_decimal('amt_credit_limit_actual') }} as amt_credit_limit_actual,
        {{ to_decimal('amt_drawings_atm_current') }} as amt_drawings_atm_current,
        {{ to_decimal('amt_drawings_current') }} as amt_drawings_current,
        {{ to_decimal('amt_drawings_other_current') }} as amt_drawings_other_current,
        {{ to_decimal('amt_drawings_pos_current') }} as amt_drawings_pos_current,
        {{ to_decimal('amt_inst_min_regularity') }} as amt_inst_min_regularity,
        {{ to_decimal('amt_payment_current') }} as amt_payment_current,
        {{ to_decimal('amt_payment_total_current') }} as amt_payment_total_current,
        {{ to_decimal('amt_receivable_principal') }} as amt_receivable_principal,
        {{ to_decimal('amt_recivable') }} as amt_recivable,
        {{ to_decimal('amt_total_receivable') }} as amt_total_receivable,
        {{ to_bigint('cnt_drawings_atm_current') }} as cnt_drawings_atm_current,
        {{ to_bigint('cnt_drawings_current') }} as cnt_drawings_current,
        {{ to_bigint('cnt_drawings_other_current') }} as cnt_drawings_other_current,
        {{ to_bigint('cnt_drawings_pos_current') }} as cnt_drawings_pos_current,
        {{ to_bigint('cnt_instalment_mature_cum') }} as cnt_instalment_mature_cum,
        {{ clean_text('name_contract_status') }} as name_contract_status,
        {{ to_bigint('sk_dpd') }} as sk_dpd,
        {{ to_bigint('sk_dpd_def') }} as sk_dpd_def,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
