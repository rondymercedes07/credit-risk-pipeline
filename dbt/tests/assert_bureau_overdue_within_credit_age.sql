{{ config(severity='warn') }}

-- Business rule: a credit cannot have been overdue for longer than it has existed.
-- days_credit is negative (days before the application the credit was opened).
select
    sk_id_bureau,
    credit_day_overdue,
    days_credit
from {{ ref('stg_bureau') }}
where credit_day_overdue > -days_credit
