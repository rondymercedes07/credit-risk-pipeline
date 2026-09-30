-- SCD2 invariant: a version ends exactly when the next one starts (no gaps, no overlaps).
with versions as (

    select
        sk_id_prev,
        dbt_valid_to,
        lead(dbt_valid_from) over (partition by sk_id_prev order by dbt_valid_from) as next_valid_from
    from {{ ref('snp_loan_status') }}

)

select sk_id_prev
from versions
where next_valid_from is not null and dbt_valid_to != next_valid_from
