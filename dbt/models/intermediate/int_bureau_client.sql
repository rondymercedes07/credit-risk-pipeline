-- Credit bureau footprint per client. Negative debts (finding 16) are not counted as exposure,
-- and the 2 credits overdue for longer than they have existed (finding 18) are left out of the
-- overdue-days maximum.
select
    sk_id_curr,
    count(*) as n_bureau_credits,
    sum(case when credit_active = 'Active' then 1 else 0 end) as n_active_bureau_credits,
    sum(case when credit_active = 'Bad debt' then 1 else 0 end) as n_bad_debt_bureau_credits,
    sum(case when credit_active = 'Active' and amt_credit_sum_debt > 0 then amt_credit_sum_debt else 0 end) as bureau_active_debt,
    sum(case when amt_credit_sum_overdue > 0 then amt_credit_sum_overdue else 0 end) as bureau_overdue_amount,
    max(case when credit_day_overdue <= -days_credit then credit_day_overdue end) as bureau_max_day_overdue
from {{ ref('stg_bureau') }}
group by sk_id_curr
