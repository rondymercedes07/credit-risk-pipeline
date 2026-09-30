{#- Materialized as a table on purpose. It aggregates 13.6M source rows and feeds fct_installments, two int models and ~10 tests: a view recomputed that aggregation on every reference (13 to 32 s per test on an XS warehouse). -#}

{{ config(materialized='table') }}

{#-
    Collapses installments_payments to one row per SCHEDULED installment, (sk_id_prev,
    num_instalment_number). The source has one row per payment AND per version, and it repeats the
    same payment in every version while splitting the amount due across versions (for example
    11,667.195 + 32.805 = 11,700 paid once). So: amount due = sum over versions, payments are
    taken once from the lowest version. See docs/data_quality_findings.md (finding 13) and
    docs/modeling_decisions.md (sections 2 and 4).

    DPD = last payment day - due day, floored at 0. Unpaid installments keep dpd NULL.
-#}

with payments as (

    select * from {{ ref('stg_installments_payments') }}

),

versions as (

    select
        sk_id_prev,
        num_instalment_number,
        num_instalment_version,
        max(sk_id_curr) as sk_id_curr,
        max(days_instalment) as days_instalment,
        max(amt_instalment) as amt_instalment,
        max(loaded_at) as loaded_at
    from payments
    group by sk_id_prev, num_instalment_number, num_instalment_version

),

due as (

    select
        sk_id_prev,
        num_instalment_number,
        max(sk_id_curr) as sk_id_curr,
        min(num_instalment_version) as first_version,
        count(*) as n_versions,
        sum(amt_instalment) as amt_due,
        max(loaded_at) as loaded_at
    from versions
    group by sk_id_prev, num_instalment_number

),

due_day as (

    select
        v.sk_id_prev,
        v.num_instalment_number,
        v.days_instalment as due_day
    from versions as v
    inner join due as d
        on
            v.sk_id_prev = d.sk_id_prev
            and v.num_instalment_number = d.num_instalment_number
            and v.num_instalment_version = d.first_version

),

paid as (

    select
        p.sk_id_prev,
        p.num_instalment_number,
        count(p.amt_payment) as n_payments,
        sum(p.amt_payment) as amt_paid,
        min(p.days_entry_payment) as first_payment_day,
        max(p.days_entry_payment) as last_payment_day
    from payments as p
    inner join due as d
        on
            p.sk_id_prev = d.sk_id_prev
            and p.num_instalment_number = d.num_instalment_number
            and p.num_instalment_version = d.first_version
    group by p.sk_id_prev, p.num_instalment_number

),

joined as (

    select
        d.sk_id_prev,
        d.sk_id_curr,
        d.num_instalment_number,
        d.n_versions,
        d.amt_due,
        d.loaded_at,
        dd.due_day,
        p.n_payments,
        p.amt_paid,
        p.first_payment_day,
        p.last_payment_day,
        p.last_payment_day - dd.due_day as days_past_due_raw
    from due as d
    inner join due_day as dd
        on d.sk_id_prev = dd.sk_id_prev and d.num_instalment_number = dd.num_instalment_number
    inner join paid as p
        on d.sk_id_prev = p.sk_id_prev and d.num_instalment_number = p.num_instalment_number

)

select
    {{ dbt_utils.generate_surrogate_key(['sk_id_prev', 'num_instalment_number']) }} as installment_id,
    sk_id_prev,
    sk_id_curr,
    num_instalment_number,
    n_versions,
    n_payments,
    due_day,
    amt_due,
    amt_paid,
    first_payment_day,
    last_payment_day,
    days_past_due_raw,
    case when days_past_due_raw is null then null else greatest(days_past_due_raw, 0) end as dpd,
    {{ dpd_bucket('days_past_due_raw') }} as dpd_bucket,
    last_payment_day is null as is_unpaid,
    coalesce(n_payments > 0 and amt_paid < amt_due - 0.01, false) as is_underpaid,
    coalesce(n_payments > 0 and amt_paid > amt_due + 0.01, false) as is_overpaid,
    case when n_payments > 0 and amt_paid < amt_due - 0.01 then amt_due - amt_paid end as amt_shortfall,
    loaded_at
from joined
