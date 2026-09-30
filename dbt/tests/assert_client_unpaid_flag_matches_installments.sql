-- Business rule: the client-level unpaid counts (n_unpaid_installments, has_unpaid_installment)
-- must agree with the installment fact for every train client, and the unpaid bucket of
-- mart_credit_risk must hold exactly the clients with an unpaid installment.
with from_fact as (

    select
        sk_id_curr,
        sum(case when is_unpaid then 1 else 0 end) as n_unpaid
    from {{ ref('fct_installments') }}
    group by sk_id_curr

),

mismatch as (

    select h.sk_id_curr
    from {{ ref('int_client_credit_history') }} as h
    inner join {{ ref('dim_client') }} as d on h.sk_id_curr = d.sk_id_curr and d.client_source = 'train'
    left join from_fact as f on h.sk_id_curr = f.sk_id_curr
    where
        h.n_unpaid_installments != coalesce(f.n_unpaid, 0)
        or h.has_unpaid_installment != (h.n_unpaid_installments > 0)

),

bucket as (

    select
        (select coalesce(sum(n_clients), 0) from {{ ref('mart_credit_risk') }}
            where dimension = 'dpd_bucket' and dimension_value = 'unpaid') as in_mart,
        (select count(*) from {{ ref('int_client_credit_history') }} as h
            inner join {{ ref('dim_client') }} as d on h.sk_id_curr = d.sk_id_curr and d.client_source = 'train'
            where h.has_unpaid_installment) as in_history

)

select cast(sk_id_curr as bigint) as sk_id_curr, null as in_mart, null as in_history from mismatch
union all
select null, in_mart, in_history from bucket where in_mart != in_history
