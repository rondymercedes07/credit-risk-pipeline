{{ config(severity='warn') }}

-- Business rule: what was paid for an installment should equal the amount due. Partial
-- payments create several rows per installment, so compare the SUM of payments per
-- (loan, version, number) with the amount due. Installments with no recorded payment
-- (amt_payment NULL) are a separate finding and are not counted here. Tolerance: 1 cent.
select
    sk_id_prev,
    num_instalment_version,
    num_instalment_number,
    sum(amt_payment) as amt_paid,
    max(amt_instalment) as amt_due
from {{ ref('stg_installments_payments') }}
group by sk_id_prev, num_instalment_version, num_instalment_number
having abs(sum(amt_payment) - max(amt_instalment)) > 0.01
