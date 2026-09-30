-- Business rule: fct_installments must contain exactly the scheduled installments of the train
-- clients, with the same amounts and unpaid counts as int_installment_schedule. This is the guard
-- of the incremental merge: a lost or duplicated row after an incremental run shows up here.
with expected as (

    select
        count(*) as n_installments,
        sum(s.amt_due) as amt_due,
        sum(coalesce(s.amt_paid, 0)) as amt_paid,
        sum(case when s.is_unpaid then 1 else 0 end) as n_unpaid
    from {{ ref('int_installment_schedule') }} as s
    inner join {{ ref('stg_application_train') }} as t on s.sk_id_curr = t.sk_id_curr

),

actual as (

    select
        count(*) as n_installments,
        sum(amt_due) as amt_due,
        sum(coalesce(amt_paid, 0)) as amt_paid,
        sum(case when is_unpaid then 1 else 0 end) as n_unpaid
    from {{ ref('fct_installments') }}

)

select
    e.n_installments as expected_installments,
    a.n_installments as actual_installments
from expected as e
cross join actual as a
where
    e.n_installments != a.n_installments
    or abs(e.amt_due - a.amt_due) > 0.01
    or abs(e.amt_paid - a.amt_paid) > 0.01
    or e.n_unpaid != a.n_unpaid
