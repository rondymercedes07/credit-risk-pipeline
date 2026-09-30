{#-
    Vintage curves on POS / cash loans of train clients. There are no calendar dates in the
    source, so a cohort is the 12-month block of the loan's decision date measured in months
    BEFORE THE CLIENT'S CURRENT APPLICATION (loan age at application), NOT a calendar origination
    vintage. Months on book (MOB) = months since the loan's first observed snapshot, which is
    relative to the loan itself, so the curve shape is valid.

    Metric: cumulative share of loans that reached 30+ DPD (raw sk_dpd >= 30; the tolerance variant sk_dpd_def >= 30 covers only 768 loans) at or before MOB m.
    Denominator = loans observable at m (right-censoring: a loan decided 5 months before the
    application can only be observed to MOB 5). Left-censored loans (first snapshot at the start of
    the window, month -96) are excluded. See docs/modeling_decisions.md section 6.
-#}

with loans as (

    select
        f.sk_id_prev,
        f.decision_cohort,
        pos.max_mob,
        pos.first_dpd30_mob
    from {{ ref('fct_loans') }} as f
    inner join {{ ref('int_pos_cash_loan') }} as pos on f.sk_id_prev = pos.sk_id_prev
    where f.is_approved and not pos.is_left_censored

),

spine as (

    select cast(generated_number - 1 as bigint) as months_on_book
    from ({{ dbt_utils.generate_series(96) }}) as numbers

),

cohort_size as (

    select
        decision_cohort,
        count(*) as n_loans_in_cohort
    from loans
    group by decision_cohort

),

curve as (

    select
        l.decision_cohort as cohort,
        s.months_on_book,
        count(*) as n_loans_observable,
        sum(case when l.first_dpd30_mob <= s.months_on_book then 1 else 0 end) as n_loans_ever_30p
    from loans as l
    inner join spine as s on l.max_mob >= s.months_on_book
    group by l.decision_cohort, s.months_on_book

)

select
    c.cohort,
    c.months_on_book,
    z.n_loans_in_cohort,
    c.n_loans_observable,
    c.n_loans_ever_30p,
    cast(c.n_loans_ever_30p as double) / c.n_loans_observable as cumulative_30p_rate
from curve as c
inner join cohort_size as z on c.cohort = z.decision_cohort
