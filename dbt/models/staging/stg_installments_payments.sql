{#-
    The source has no primary key: (sk_id_prev, num_instalment_version, num_instalment_number)
    repeats whenever an installment is paid in several parts. installment_payment_id hashes all
    eight business columns of the RAW row, so it is stable across reloads and unique as long as
    there are no exact duplicate rows (there are none in the real data; the unique test on the
    id fails loudly if a future load introduces one).
-#}

with source as (

    select * from {{ source('raw', 'installments_payments') }}

),

typed as (

    select
        {{ dbt_utils.generate_surrogate_key(['sk_id_prev', 'sk_id_curr', 'num_instalment_version', 'num_instalment_number', 'days_instalment', 'days_entry_payment', 'amt_instalment', 'amt_payment']) }} as installment_payment_id,
        {{ to_bigint('sk_id_prev') }} as sk_id_prev,
        {{ to_bigint('sk_id_curr') }} as sk_id_curr,
        {{ to_bigint('num_instalment_version') }} as num_instalment_version,
        {{ to_bigint('num_instalment_number') }} as num_instalment_number,
        {{ to_bigint('days_instalment') }} as days_instalment,
        {{ to_bigint('days_entry_payment') }} as days_entry_payment,
        {{ to_decimal('amt_instalment') }} as amt_instalment,
        {{ to_decimal('amt_payment') }} as amt_payment,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
