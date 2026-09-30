-- Payment behaviour of each loan, rolled up from its scheduled installments. Includes loans
-- without a previous_application row (orphans); fct_loans decides which ones it keeps.
select
    sk_id_prev,
    max(sk_id_curr) as sk_id_curr,
    count(*) as n_installments,
    sum(case when is_unpaid then 1 else 0 end) as n_unpaid_installments,
    sum(case when dpd > 0 then 1 else 0 end) as n_late_installments,
    sum(case when dpd > 30 then 1 else 0 end) as n_late_30p_installments,
    max(dpd) as max_dpd,
    {{ dpd_bucket('max(dpd)') }} as dpd_bucket,
    sum(amt_due) as amt_due_total,
    sum(amt_paid) as amt_paid_total
from {{ ref('int_installment_schedule') }}
group by sk_id_prev
