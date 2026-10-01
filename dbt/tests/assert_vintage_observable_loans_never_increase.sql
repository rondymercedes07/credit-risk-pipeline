-- Business rule: within a cohort, the loans observable at month on book m can only shrink as m
-- grows (right-censoring), and month 0 holds the whole cohort. A violation means the curve
-- mixes different loan populations.
with curve as (

    select
        cohort,
        months_on_book,
        n_loans_observable,
        n_loans_in_cohort,
        lag(n_loans_observable) over (partition by cohort order by months_on_book) as previous_observable
    from {{ ref('mart_vintage_curves') }}

)

select
    cohort,
    months_on_book,
    n_loans_observable,
    previous_observable
from curve
where
    (previous_observable is not null and n_loans_observable > previous_observable)
    or (months_on_book = 0 and n_loans_observable != n_loans_in_cohort)
