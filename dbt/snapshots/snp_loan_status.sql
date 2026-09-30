{% snapshot snp_loan_status %}

{{
    config(
        target_schema='marts',
        unique_key='sk_id_prev',
        strategy='check',
        check_cols=['name_contract_status']
    )
}}

select * from {{ ref('int_loan_status_as_of') }}

{% endsnapshot %}
