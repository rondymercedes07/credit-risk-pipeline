with source as (

    select * from {{ source('raw', 'pos_cash_balance') }}

),

typed as (

    select
        {{ to_bigint('sk_id_prev') }} as sk_id_prev,
        {{ to_bigint('sk_id_curr') }} as sk_id_curr,
        {{ to_bigint('months_balance') }} as months_balance,
        {{ to_bigint('cnt_instalment') }} as cnt_instalment,
        {{ to_bigint('cnt_instalment_future') }} as cnt_instalment_future,
        {{ null_if_xna('name_contract_status') }} as name_contract_status,
        {{ is_xna('name_contract_status') }} as is_name_contract_status_xna,
        {{ to_bigint('sk_dpd') }} as sk_dpd,
        {{ to_bigint('sk_dpd_def') }} as sk_dpd_def,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
