{#-
    One row per client of application_train (labelled) and application_test (unlabelled).
    is_default is the TARGET flag of the CURRENT application: a client-level flag, NULL for test
    clients. Segment columns (age_band, income_band) use fixed thresholds instead of quantiles so
    they are deterministic and identical on Snowflake and DuckDB.
-#}

{% set attributes = [
    'sk_id_curr', 'name_contract_type', 'code_gender', 'days_birth', 'cnt_children', 'amt_income_total',
    'amt_credit', 'amt_annuity', 'amt_goods_price', 'name_income_type', 'name_education_type',
    'name_family_status', 'name_housing_type', 'occupation_type', 'organization_type',
    'region_rating_client', 'days_employed', 'is_days_employed_sentinel', 'flag_own_car', 'flag_own_realty',
] %}

with applications as (

    select
        'train' as client_source,
        target,
        {{ attributes | join(', ') }}
    from {{ ref('stg_application_train') }}

    union all

    select
        'test' as client_source,
        cast(null as bigint) as target,
        {{ attributes | join(', ') }}
    from {{ ref('stg_application_test') }}

)

select
    sk_id_curr,
    client_source,
    case when target is null then null else target = 1 end as is_default,
    name_contract_type,
    code_gender,
    cast(floor(-days_birth / 365.25) as bigint) as age_years,
    case
        when days_birth is null then 'unknown'
        when -days_birth / 365.25 < 25 then '<25'
        when -days_birth / 365.25 < 35 then '25-34'
        when -days_birth / 365.25 < 45 then '35-44'
        when -days_birth / 365.25 < 55 then '45-54'
        when -days_birth / 365.25 < 65 then '55-64'
        else '65+'
    end as age_band,
    cnt_children,
    amt_income_total,
    case
        when amt_income_total is null then 'unknown'
        when amt_income_total < 90000 then '1: <90k'
        when amt_income_total < 135000 then '2: 90k-135k'
        when amt_income_total < 180000 then '3: 135k-180k'
        when amt_income_total < 270000 then '4: 180k-270k'
        else '5: 270k+'
    end as income_band,
    amt_credit,
    amt_annuity,
    amt_goods_price,
    name_income_type,
    name_education_type,
    name_family_status,
    name_housing_type,
    occupation_type,
    organization_type,
    region_rating_client,
    days_employed,
    is_days_employed_sentinel,
    flag_own_car,
    flag_own_realty
from applications
