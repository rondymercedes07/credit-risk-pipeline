-- Business rule: fct_loans has exactly one row per previous application of a train client, and
-- its installment counts add up to the installments of the loans that have a loan record.
with expected as (

    select count(*) as n_loans
    from {{ ref('stg_previous_application') }} as p
    inner join {{ ref('stg_application_train') }} as t on p.sk_id_curr = t.sk_id_curr

),

loans as (

    select
        count(*) as n_loans,
        sum(n_installments) as n_installments
    from {{ ref('fct_loans') }}

),

installments as (

    select count(*) as n_installments
    from {{ ref('fct_installments') }}
    where has_loan_record

)

select
    e.n_loans as expected_loans,
    l.n_loans as actual_loans,
    l.n_installments as loan_installments,
    i.n_installments as fact_installments
from expected as e
cross join loans as l
cross join installments as i
where e.n_loans != l.n_loans or l.n_installments != i.n_installments
