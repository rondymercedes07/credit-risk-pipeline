{#-
    Status of every POS / cash loan AS OF a simulated load month: its latest snapshot with
    months_balance <= var('as_of_month') (default -1 = all history). Loans not yet observed at
    that month are absent, as they would be in a real monthly load. This is the source of the
    snp_loan_status snapshot; the replay script sets as_of_month from -96 to -1.

    Ephemeral on purpose: the variable must be evaluated at every snapshot run, and a view would
    freeze the value it had when it was created.
-#}

{{ config(materialized='ephemeral') }}

with ranked as (

    select
        sk_id_prev,
        sk_id_curr,
        name_contract_status,
        months_balance,
        row_number() over (partition by sk_id_prev order by months_balance desc) as recency_rank
    from {{ ref('stg_pos_cash_balance') }}
    where months_balance <= {{ var('as_of_month', -1) }}

)

select
    sk_id_prev,
    sk_id_curr,
    name_contract_status,
    months_balance as observed_month,
    {{ var('as_of_month', -1) }} as as_of_month
from ranked
where recency_rank = 1
