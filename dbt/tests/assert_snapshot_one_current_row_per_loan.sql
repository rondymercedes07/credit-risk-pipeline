-- SCD2 invariant: every loan has exactly one current version (dbt_valid_to is NULL).
select sk_id_prev
from {{ ref('snp_loan_status') }}
group by sk_id_prev
having sum(case when dbt_valid_to is null then 1 else 0 end) != 1
