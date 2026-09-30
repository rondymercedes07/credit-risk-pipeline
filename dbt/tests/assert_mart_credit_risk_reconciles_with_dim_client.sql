-- Business rule: every segment dimension of mart_credit_risk is a partition of the train clients,
-- so for EACH dimension the clients, the defaults and the exposure must add up to the totals of
-- dim_client (client_source = 'train'). Catches dropped NULL segments, double counting and test
-- clients leaking into the risk universe. Returns one row per dimension that does not reconcile.
with expected as (

    select
        count(*) as n_clients,
        sum(case when is_default then 1 else 0 end) as n_defaults,
        sum(coalesce(amt_credit, 0)) as exposure
    from {{ ref('dim_client') }}
    where client_source = 'train'

),

per_dimension as (

    select
        dimension,
        sum(n_clients) as n_clients,
        sum(n_defaults) as n_defaults,
        sum(exposure_application_credit) as exposure
    from {{ ref('mart_credit_risk') }}
    group by dimension

)

select
    p.dimension,
    p.n_clients,
    e.n_clients as expected_clients,
    p.n_defaults,
    e.n_defaults as expected_defaults,
    p.exposure,
    e.exposure as expected_exposure
from per_dimension as p
cross join expected as e
where
    p.n_clients != e.n_clients
    or p.n_defaults != e.n_defaults
    or abs(p.exposure - e.exposure) > 0.01
