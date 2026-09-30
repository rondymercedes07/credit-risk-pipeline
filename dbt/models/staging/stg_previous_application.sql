with source as (

    select * from {{ source('raw', 'previous_application') }}

),

typed as (

    select
        {{ to_bigint('sk_id_prev') }} as sk_id_prev,
        {{ to_bigint('sk_id_curr') }} as sk_id_curr,
        {{ null_if_xna('name_contract_type') }} as name_contract_type,
        {{ is_xna('name_contract_type') }} as is_name_contract_type_xna,
        {{ to_decimal('amt_annuity') }} as amt_annuity,
        {{ to_decimal('amt_application') }} as amt_application,
        {{ to_decimal('amt_credit') }} as amt_credit,
        {{ to_decimal('amt_down_payment') }} as amt_down_payment,
        {{ to_decimal('amt_goods_price') }} as amt_goods_price,
        {{ clean_text('weekday_appr_process_start') }} as weekday_appr_process_start,
        {{ to_bigint('hour_appr_process_start') }} as hour_appr_process_start,
        {{ to_bool_yn('flag_last_appl_per_contract') }} as flag_last_appl_per_contract,
        {{ to_bool_01('nflag_last_appl_in_day') }} as nflag_last_appl_in_day,
        {{ to_double('rate_down_payment') }} as rate_down_payment,
        {{ to_double('rate_interest_primary') }} as rate_interest_primary,
        {{ to_double('rate_interest_privileged') }} as rate_interest_privileged,
        {{ null_if_xna('name_cash_loan_purpose') }} as name_cash_loan_purpose,
        {{ is_xna('name_cash_loan_purpose') }} as is_name_cash_loan_purpose_xna,
        {{ clean_text('name_contract_status') }} as name_contract_status,
        {{ to_bigint('days_decision') }} as days_decision,
        {{ null_if_xna('name_payment_type') }} as name_payment_type,
        {{ is_xna('name_payment_type') }} as is_name_payment_type_xna,
        {{ null_if_xna('code_reject_reason') }} as code_reject_reason,
        {{ is_xna('code_reject_reason') }} as is_code_reject_reason_xna,
        {{ clean_text('name_type_suite') }} as name_type_suite,
        {{ null_if_xna('name_client_type') }} as name_client_type,
        {{ is_xna('name_client_type') }} as is_name_client_type_xna,
        {{ null_if_xna('name_goods_category') }} as name_goods_category,
        {{ is_xna('name_goods_category') }} as is_name_goods_category_xna,
        {{ null_if_xna('name_portfolio') }} as name_portfolio,
        {{ is_xna('name_portfolio') }} as is_name_portfolio_xna,
        {{ null_if_xna('name_product_type') }} as name_product_type,
        {{ is_xna('name_product_type') }} as is_name_product_type_xna,
        {{ clean_text('channel_type') }} as channel_type,
        {{ to_bigint('sellerplace_area') }} as sellerplace_area,
        {{ null_if_xna('name_seller_industry') }} as name_seller_industry,
        {{ is_xna('name_seller_industry') }} as is_name_seller_industry_xna,
        {{ to_bigint('cnt_payment') }} as cnt_payment,
        {{ null_if_xna('name_yield_group') }} as name_yield_group,
        {{ is_xna('name_yield_group') }} as is_name_yield_group_xna,
        {{ clean_text('product_combination') }} as product_combination,
        {{ null_if_sentinel('days_first_drawing') }} as days_first_drawing,
        {{ is_sentinel('days_first_drawing') }} as is_days_first_drawing_sentinel,
        {{ null_if_sentinel('days_first_due') }} as days_first_due,
        {{ is_sentinel('days_first_due') }} as is_days_first_due_sentinel,
        {{ null_if_sentinel('days_last_due_1st_version') }} as days_last_due_1st_version,
        {{ is_sentinel('days_last_due_1st_version') }} as is_days_last_due_1st_version_sentinel,
        {{ null_if_sentinel('days_last_due') }} as days_last_due,
        {{ is_sentinel('days_last_due') }} as is_days_last_due_sentinel,
        {{ null_if_sentinel('days_termination') }} as days_termination,
        {{ is_sentinel('days_termination') }} as is_days_termination_sentinel,
        {{ to_bool_01('nflag_insured_on_approval') }} as nflag_insured_on_approval,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
