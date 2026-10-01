{#-
    Default rate and exposure of the TRAIN clients (the only ones with TARGET), sliced by each
    segment dimension in turn. Long format: one row per (dimension, dimension_value), so every
    dimension partitions the train clients exactly once (NULL becomes 'unknown') and each dimension
    sums to the same totals; business tests assert that.

    The default flag is a client-level flag, so all rates are per client, never per loan.
    default_rate_ci_low / _high: Wilson 95% interval, because some buckets are small (for example
    61-90 DPD has 1,598 clients) and a point rate alone would overstate precision.
-#}

{% set dimensions = [
    'name_contract_type', 'code_gender', 'age_band', 'income_band', 'name_education_type',
    'name_income_type', 'region_rating_client', 'tenure_cohort', 'dpd_bucket', 'worst_dpd_bucket_12m',
] %}
{#- The first seven come from dim_client, the rest from int_client_credit_history. -#}
{% set dim_client_dimensions = dimensions[:7] %}

with clients as (

    select
        d.sk_id_curr,
        d.is_default,
        d.amt_credit,
        h.bureau_active_debt,
    {% for dim in dimensions %}
        coalesce(
            cast(
                {{ 'd' if dim in dim_client_dimensions else 'h' }}.{{ dim }} as varchar
            ),
            'unknown'
        ) as {{ dim }}
        {{- ',' if not loop.last }}
    {% endfor %}
    from {{ ref('dim_client') }} as d
    inner join {{ ref('int_client_credit_history') }} as h on d.sk_id_curr = h.sk_id_curr
    where d.client_source = 'train'

),

stacked as (

    {% for dim in dimensions %}
        select
            '{{ dim }}' as dimension,
            {{ dim }} as dimension_value,
            is_default,
            amt_credit,
            bureau_active_debt
        from clients
        {{ 'union all' if not loop.last }}
    {% endfor %}

),

aggregated as (

    select
        dimension,
        dimension_value,
        count(*) as n_clients,
        sum(case when is_default then 1 else 0 end) as n_defaults,
        sum(coalesce(amt_credit, 0)) as exposure_application_credit,
        sum(bureau_active_debt) as exposure_bureau_active_debt
    from stacked
    group by dimension, dimension_value

),

rates as (

    select
        *,
        cast(n_defaults as double) / n_clients as default_rate,
        cast(n_clients as double) / sum(n_clients) over (partition by dimension) as share_of_clients
    from aggregated

)

select
    dimension,
    dimension_value,
    n_clients,
    n_defaults,
    default_rate,
    (
        default_rate + 1.96 * 1.96 / (2 * n_clients)
        - 1.96 * sqrt(default_rate * (1 - default_rate) / n_clients + 1.96 * 1.96 / (4 * n_clients * n_clients))
    )
    / (1 + 1.96 * 1.96 / n_clients) as default_rate_ci_low,
    (
        default_rate + 1.96 * 1.96 / (2 * n_clients)
        + 1.96 * sqrt(default_rate * (1 - default_rate) / n_clients + 1.96 * 1.96 / (4 * n_clients * n_clients))
    )
    / (1 + 1.96 * 1.96 / n_clients) as default_rate_ci_high,
    share_of_clients,
    exposure_application_credit,
    exposure_bureau_active_debt
from rates
