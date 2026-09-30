-- Independent reconciliation of the simulated SCD2: the number of versions of each loan must equal
-- the number of status runs in its POS history, counted straight from staging. The replay window is
-- read from the snapshot itself (min / max as_of_month): the sequence is the latest status at or
-- before the first loaded month, followed by every later month up to the last loaded one. Fails if
-- the replay skipped months or the snapshot logic loses a change.
with bounds as (

    select
        min(as_of_month) as start_month,
        max(as_of_month) as end_month
    from {{ ref('snp_loan_status') }}

),

history as (

    select
        p.sk_id_prev,
        p.months_balance,
        b.start_month,
        coalesce(p.name_contract_status, '(null)') as status,
        row_number() over (
            partition by p.sk_id_prev, case when p.months_balance <= b.start_month then 0 else 1 end
            order by p.months_balance desc
        ) as rank_in_side
    from {{ ref('stg_pos_cash_balance') }} as p
    cross join bounds as b
    where p.months_balance <= b.end_month

),

kept as (

    select
        sk_id_prev,
        status,
        lag(status) over (partition by sk_id_prev order by months_balance) as previous_status
    from history
    where months_balance > start_month or rank_in_side = 1

),

expected as (

    select
        sk_id_prev,
        sum(case when previous_status is null or previous_status != status then 1 else 0 end) as expected_versions
    from kept
    group by sk_id_prev

),

actual as (

    select
        sk_id_prev,
        count(*) as actual_versions
    from {{ ref('snp_loan_status') }}
    group by sk_id_prev

)

select
    e.sk_id_prev,
    e.expected_versions,
    a.actual_versions
from expected as e
left join actual as a on e.sk_id_prev = a.sk_id_prev
where a.actual_versions is null or e.expected_versions != a.actual_versions
