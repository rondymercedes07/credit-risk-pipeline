# Headline metrics for the README

Numbers measured on the **full dataset** in Snowflake (`CREDIT_RISK_DEV`, marts schema), phase 3 state. The phase 5
README takes them from here. Each number comes with the query that produces it. Amounts are in **dataset currency
units**: the source does not say which currency, so no currency symbol is ever used.

## 1. Global default rate and exposure

**8.07% of clients defaulted** (24,825 of 307,511 train clients), on **184.2 billion dataset currency units** of credit
requested in the current applications.

```sql
select
    sum(n_clients) as n_clients,
    sum(n_defaults) as n_defaults,
    round(100 * sum(n_defaults) / sum(n_clients), 2) as default_rate_pct,
    round(sum(exposure_application_credit) / 1e9, 1) as exposure_bn_dataset_currency_units
from marts.mart_credit_risk
where dimension = 'code_gender';   -- any single dimension partitions the train clients exactly once
```

Result: `307511 | 24825 | 8.07 | 184.2`. Default is a client-level flag (TARGET of the current application).

## 2. Clients with unpaid installments: 18.14%, 2.2x the average

**1,075 clients have at least one installment with no payment; 18.14% of them defaulted, 2.2 times the average rate.**

```sql
with overall as (
    select sum(n_defaults) / sum(n_clients) as rate
    from marts.mart_credit_risk where dimension = 'name_contract_type'
)
select r.n_clients, round(100 * r.default_rate, 2) as default_rate_pct,
       round(r.default_rate / o.rate, 1) as times_average
from marts.mart_credit_risk as r cross join overall as o
where r.dimension = 'dpd_bucket' and r.dimension_value = 'unpaid';
```

Result: `1075 | 18.14 | 2.2`. 95% Wilson interval 15.95% to 20.56% (columns `default_rate_ci_low` / `_high`), against 8.04% for
clients without unpaid installments.

## 3. Recency of the worst delinquency: 25.0% vs 8.11%

Among clients whose worst installment was more than 90 days late, **25.0% defaulted if that worst delay was in the 12
months before the application, against 8.11% if it was more than 24 months earlier.** The lifetime "90+" bucket (9.63%)
hides this gap; 89% of its clients are in the old group. Full analysis: `data_quality_findings.md`, "Investigation:
default rate by worst DPD".

```sql
with paid as (
    select sk_id_curr, due_day, dpd from marts.fct_installments where dpd is not null
),
worst as (
    select sk_id_curr, max(dpd) as max_dpd from paid group by sk_id_curr
),
worst_day as (  -- due day of the worst installment (the latest one if several tie)
    select p.sk_id_curr, max(p.due_day) as worst_due_day
    from paid as p inner join worst as w on p.sk_id_curr = w.sk_id_curr and p.dpd = w.max_dpd
    group by p.sk_id_curr
)
select
    case when d.worst_due_day >= -365 then 'worst DPD in last 12 months'
         when d.worst_due_day >= -730 then '12 to 24 months ago'
         else 'more than 24 months ago' end as worst_dpd_recency,
    count(*) as n_clients,
    round(100 * avg(case when c.is_default then 1.0 else 0.0 end), 2) as default_rate_pct
from worst as w
inner join worst_day as d on w.sk_id_curr = d.sk_id_curr
inner join marts.dim_client as c on w.sk_id_curr = c.sk_id_curr
where w.max_dpd > 90
group by 1 order by 1;
```

Result: `worst DPD in last 12 months | 164 | 25.00`, `12 to 24 months ago | 615 | 21.14`, `more than 24 months ago | 6314 | 8.11`.
Days are relative to each client's own current application, not calendar dates. Sample sizes are small in the recent
group (164 clients); say so next to the number.
