-- One row per credit card contract from its monthly balances. Revolving credit has no fixed
-- origination or term, so it is summarised but not used in vintage curves.
with monthly as (

    select
        *,
        row_number() over (partition by sk_id_prev order by months_balance desc) as recency_rank
    from {{ ref('stg_credit_card_balance') }}

),

cards as (

    select
        sk_id_prev,
        max(sk_id_curr) as sk_id_curr,
        min(months_balance) as first_month,
        max(months_balance) as last_month,
        count(*) as n_months_observed,
        max(sk_dpd) as max_sk_dpd,
        max(sk_dpd_def) as max_sk_dpd_def,
        max(case when amt_credit_limit_actual > 0 then amt_balance / amt_credit_limit_actual end) as max_utilization
    from monthly
    group by sk_id_prev

),

latest as (

    select
        sk_id_prev,
        amt_balance as last_amt_balance,
        amt_credit_limit_actual as last_credit_limit,
        name_contract_status as last_status
    from monthly
    where recency_rank = 1

)

select
    c.sk_id_prev,
    c.sk_id_curr,
    c.first_month,
    c.last_month,
    c.n_months_observed,
    c.max_sk_dpd,
    c.max_sk_dpd_def,
    c.max_utilization,
    l.last_amt_balance,
    l.last_credit_limit,
    l.last_status
from cards as c
inner join latest as l on c.sk_id_prev = l.sk_id_prev
