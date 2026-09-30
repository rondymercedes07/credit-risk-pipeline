{#-
    INCREMENTAL STRATEGY (docs/modeling_decisions.md section 8)

    - merge on installment_id, not append: an installment already loaded can change later (a
      payment arrives for a row that had none, a partial payment is added), and append would
      keep the stale row next to the new one. insert_overwrite is out: there are no date
      partitions.
    - watermark: loaded_at, the RAW audit column, which int_installment_schedule carries as the
      MAX over every source row of the installment. An installment with a new payment row
      therefore passes the filter and arrives fully re-aggregated (old + new payments together),
      never aggregated from the delta alone.
    - honest limit: scripts/load_raw.py reloads each table in full, so every loaded_at changes
      on every load and the filter then selects everything (a full, idempotent re-merge). The
      int view also aggregates all source rows on every run, so the saving today is on the write
      side only. It pays off when the loader appends deltas (Airflow, phase 4).
    - on_schema_change='fail': a silent column drift in a 11M-row fact should stop the run.

    Grain: one row per scheduled installment of a TRAIN client. Installments of loans that have
    no previous_application row are kept with has_loan_record = false: each row carries its own
    sk_id_curr, so client-level delinquency does not lose them.
-#}

{{
    config(
        materialized='incremental',
        unique_key='installment_id',
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

select
    s.installment_id,
    s.sk_id_prev,
    s.sk_id_curr,
    s.num_instalment_number,
    p.sk_id_prev is not null as has_loan_record,
    s.n_versions,
    s.n_payments,
    s.due_day,
    s.amt_due,
    s.amt_paid,
    s.amt_shortfall,
    s.first_payment_day,
    s.last_payment_day,
    s.days_past_due_raw,
    s.dpd,
    s.dpd_bucket,
    s.is_unpaid,
    s.is_underpaid,
    s.is_overpaid,
    coalesce(s.last_payment_day < p.days_decision, false) as is_paid_before_decision,
    s.loaded_at
from {{ ref('int_installment_schedule') }} as s
inner join {{ ref('stg_application_train') }} as t on s.sk_id_curr = t.sk_id_curr
left join {{ ref('stg_previous_application') }} as p on s.sk_id_prev = p.sk_id_prev
{% if is_incremental() %}
    where s.loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
