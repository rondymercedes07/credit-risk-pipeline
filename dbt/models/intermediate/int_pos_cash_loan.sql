{#-
    One row per POS / cash loan from its monthly snapshots. months_balance is relative to the
    current application (-1 = last month before it, -96 = start of the window), so months on
    book (MOB) of a snapshot = months_balance - first observed month. A loan whose first
    snapshot is month -96 may have started earlier: it is flagged left-censored and excluded
    from vintage curves. sk_dpd_def is DPD "with tolerance" (small amounts ignored); it reaches
    30+ in only 768 of 936,325 loans, too few for a curve, so the vintage 30+ event uses raw sk_dpd.
-#}

with monthly as (

    select
        *,
        row_number() over (partition by sk_id_prev order by months_balance desc) as recency_rank
    from {{ ref('stg_pos_cash_balance') }}

),

loans as (

    select
        sk_id_prev,
        max(sk_id_curr) as sk_id_curr,
        min(months_balance) as first_month,
        max(months_balance) as last_month,
        count(*) as n_months_observed,
        max(cnt_instalment) as cnt_instalment,
        max(sk_dpd) as max_sk_dpd,
        max(sk_dpd_def) as max_sk_dpd_def,
        min(case when sk_dpd >= 30 then months_balance end) as first_dpd30_month
    from monthly
    group by sk_id_prev

),

last_status as (

    select
        sk_id_prev,
        name_contract_status as last_status
    from monthly
    where recency_rank = 1

)

select
    l.sk_id_prev,
    l.sk_id_curr,
    l.first_month,
    l.last_month,
    l.n_months_observed,
    l.last_month - l.first_month as max_mob,
    l.first_month = -96 as is_left_censored,
    l.cnt_instalment,
    l.max_sk_dpd,
    l.max_sk_dpd_def,
    l.first_dpd30_month - l.first_month as first_dpd30_mob,
    s.last_status
from loans as l
inner join last_status as s on l.sk_id_prev = s.sk_id_prev
