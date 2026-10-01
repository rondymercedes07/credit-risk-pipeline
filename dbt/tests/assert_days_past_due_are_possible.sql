-- Business rule (hard): days past due cannot be negative, and the "with tolerance" variant
-- (sk_dpd_def) can never exceed the raw one (sk_dpd). Fails the build if violated.
select
    'pos_cash_balance' as source_table,
    sk_id_prev as entity_id,
    months_balance
from {{ ref('stg_pos_cash_balance') }}
where sk_dpd < 0 or sk_dpd_def < 0 or sk_dpd_def > sk_dpd

union all

select
    'credit_card_balance',
    sk_id_prev,
    months_balance
from {{ ref('stg_credit_card_balance') }}
where sk_dpd < 0 or sk_dpd_def < 0 or sk_dpd_def > sk_dpd

union all

select
    'bureau',
    sk_id_bureau,
    null
from {{ ref('stg_bureau') }}
where credit_day_overdue < 0
