{#-
    One row per previous Home Credit application of a TRAIN client (all statuses: approved,
    refused, canceled, unused offer). Payment, POS and card signals are LEFT joined, so a refused
    loan simply has no installments. The default flag is intentionally NOT here: TARGET is a
    client-level flag, so it lives on dim_client and is never averaged per loan.
    Loans that appear in the payment tables without a previous_application row cannot be in this
    table (no decision date, type or status); fct_installments keeps their installments.
-#}

select
    p.sk_id_prev,
    p.sk_id_curr,
    p.name_contract_type,
    p.name_contract_status,
    p.name_portfolio,
    p.name_product_type,
    p.amt_application,
    p.amt_credit,
    p.amt_down_payment,
    p.amt_goods_price,
    p.amt_annuity,
    p.cnt_payment,
    p.days_decision,
    {{ months_before_application('p.days_decision') }} as loan_age_months,
    {{ cohort_block(months_before_application('p.days_decision')) }} as decision_cohort,
    p.name_contract_status = 'Approved' as is_approved,
    coalesce(perf.n_installments, 0) as n_installments,
    coalesce(perf.n_unpaid_installments, 0) as n_unpaid_installments,
    coalesce(perf.n_late_installments, 0) as n_late_installments,
    coalesce(perf.n_late_30p_installments, 0) as n_late_30p_installments,
    perf.max_dpd,
    perf.dpd_bucket,
    perf.amt_due_total,
    perf.amt_paid_total,
    pos.sk_id_prev is not null as has_pos_history,
    pos.max_sk_dpd_def as pos_max_sk_dpd_def,
    cc.sk_id_prev is not null as has_card_history,
    cc.max_sk_dpd_def as cc_max_sk_dpd_def
from {{ ref('stg_previous_application') }} as p
inner join {{ ref('stg_application_train') }} as t on p.sk_id_curr = t.sk_id_curr
left join {{ ref('int_loan_payment_performance') }} as perf on p.sk_id_prev = perf.sk_id_prev
left join {{ ref('int_pos_cash_loan') }} as pos on p.sk_id_prev = pos.sk_id_prev
left join {{ ref('int_credit_card_loan') }} as cc on p.sk_id_prev = cc.sk_id_prev
