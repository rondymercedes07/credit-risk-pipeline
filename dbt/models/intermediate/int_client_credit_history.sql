{#-
    Materialized as a table on purpose. It joins five aggregates and is read by the risk mart and
    ~6 tests: a view recomputed everything on every reference.
-#}

{{ config(materialized='table') }}

{#-
    One row per client (train and test) with everything the risk marts need about their past:
    worst DPD from installments (paid ones; unpaid are counted apart, decision in
    docs/modeling_decisions.md section 4), tenure with Home Credit, POS / card / bureau signals.
    Clients without a given history get NULLs or 0 counts, never dropped.
-#}

with clients as (

    select sk_id_curr from {{ ref('stg_application_train') }}
    union all
    select sk_id_curr from {{ ref('stg_application_test') }}

),

installments as (

    select
        sk_id_curr,
        count(*) as n_installments,
        sum(case when is_unpaid then 1 else 0 end) as n_unpaid_installments,
        max(dpd) as max_dpd,
        max(case when due_day >= -365 then dpd end) as max_dpd_12m
    from {{ ref('int_installment_schedule') }}
    group by sk_id_curr

),

applications as (

    select
        sk_id_curr,
        count(*) as n_previous_applications,
        sum(case when name_contract_status = 'Approved' then 1 else 0 end) as n_approved_loans,
        min(case when name_contract_status = 'Approved' then days_decision end) as first_approved_decision_day
    from {{ ref('stg_previous_application') }}
    group by sk_id_curr

),

pos as (

    select
        sk_id_curr,
        max(max_sk_dpd_def) as pos_max_sk_dpd_def
    from {{ ref('int_pos_cash_loan') }}
    group by sk_id_curr

),

cards as (

    select
        sk_id_curr,
        max(max_sk_dpd_def) as cc_max_sk_dpd_def
    from {{ ref('int_credit_card_loan') }}
    group by sk_id_curr

),

joined as (

    select
        c.sk_id_curr,
        coalesce(i.n_installments, 0) as n_installments,
        coalesce(i.n_unpaid_installments, 0) as n_unpaid_installments,
        coalesce(i.n_unpaid_installments, 0) > 0 as has_unpaid_installment,
        i.max_dpd,
        i.max_dpd_12m,
        coalesce(a.n_previous_applications, 0) as n_previous_applications,
        coalesce(a.n_approved_loans, 0) as n_approved_loans,
        a.first_approved_decision_day,
        case
            when a.first_approved_decision_day is null then null
            else {{ months_before_application('a.first_approved_decision_day') }}
        end as tenure_months,
        p.pos_max_sk_dpd_def,
        cc.cc_max_sk_dpd_def,
        coalesce(b.n_bureau_credits, 0) as n_bureau_credits,
        coalesce(b.n_active_bureau_credits, 0) as n_active_bureau_credits,
        coalesce(b.n_bad_debt_bureau_credits, 0) as n_bad_debt_bureau_credits,
        coalesce(b.bureau_active_debt, 0) as bureau_active_debt,
        coalesce(b.bureau_overdue_amount, 0) as bureau_overdue_amount,
        b.bureau_max_day_overdue
    from clients as c
    left join installments as i on c.sk_id_curr = i.sk_id_curr
    left join applications as a on c.sk_id_curr = a.sk_id_curr
    left join pos as p on c.sk_id_curr = p.sk_id_curr
    left join cards as cc on c.sk_id_curr = cc.sk_id_curr
    left join {{ ref('int_bureau_client') }} as b on c.sk_id_curr = b.sk_id_curr

)

select
    *,
    coalesce({{ dpd_bucket('max_dpd') }}, 'no_history') as worst_dpd_bucket,
    case
        when n_installments = 0 then 'no_history'
        else coalesce({{ dpd_bucket('max_dpd_12m') }}, 'none_last_12m')
    end as worst_dpd_bucket_12m,
    case
        when n_unpaid_installments > 0 then 'unpaid'
        else coalesce({{ dpd_bucket('max_dpd') }}, 'no_history')
    end as dpd_bucket,
    case
        when tenure_months is null then 'no_prior_loan'
        else {{ cohort_block('tenure_months') }}
    end as tenure_cohort
from joined
