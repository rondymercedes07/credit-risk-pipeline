{{ config(severity='warn') }}

-- Business rule: drawings and the receivable principal cannot be negative, and neither can a
-- balance. A negative balance can be a legitimate overpayment credit, so it is reported with
-- the rest and judged in docs/data_quality_findings.md rather than hidden.
select
    sk_id_prev,
    months_balance,
    amt_balance,
    amt_drawings_current,
    amt_drawings_atm_current,
    amt_receivable_principal
from {{ ref('stg_credit_card_balance') }}
where
    amt_balance < 0
    or amt_drawings_current < 0
    or amt_drawings_atm_current < 0
    or amt_drawings_pos_current < 0
    or amt_drawings_other_current < 0
    or amt_receivable_principal < 0
