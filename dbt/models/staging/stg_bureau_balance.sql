with source as (

    select * from {{ source('raw', 'bureau_balance') }}

),

typed as (

    select
        {{ to_bigint('sk_id_bureau') }} as sk_id_bureau,
        {{ to_bigint('months_balance') }} as months_balance,
        {{ clean_text('status') }} as status,
        _loaded_at as loaded_at,
        _source_file as source_file
    from source

)

select * from typed
