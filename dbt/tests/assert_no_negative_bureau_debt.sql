{{ config(severity='warn') }}

-- Business rule: outstanding debt, credit limit and overdue amounts cannot be negative.
select sk_id_bureau, amt_credit_sum_debt, amt_credit_sum_limit, amt_credit_sum_overdue
from {{ ref('stg_bureau') }}
where amt_credit_sum_debt < 0
    or amt_credit_sum_limit < 0
    or amt_credit_sum_overdue < 0
    or amt_credit_max_overdue < 0
