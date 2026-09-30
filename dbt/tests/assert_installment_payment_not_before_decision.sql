{{ config(severity='warn') }}

-- Business rule: an installment cannot be paid before the loan application was decided.
-- days_decision is the closest thing to an origination date in the source (days relative
-- to the current application, negative = in the past).
select
    i.installment_payment_id,
    i.sk_id_prev,
    i.days_entry_payment,
    p.days_decision
from {{ ref('stg_installments_payments') }} as i
inner join {{ ref('stg_previous_application') }} as p
    on i.sk_id_prev = p.sk_id_prev
where i.days_entry_payment < p.days_decision
