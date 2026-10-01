-- Every client referenced by a related table must exist in application_train or application_test.
-- Replaces the two warn-level relationships tests to application_train: bureau and
-- previous_application also hold the clients of application_test (root cause of those orphans,
-- confirmed in docs/data_quality_findings.md, 0 unexplained). A client in neither is a real
-- integrity failure, so this test has error severity.
with known_clients as (

    select sk_id_curr from {{ ref('stg_application_train') }}
    union all
    select sk_id_curr from {{ ref('stg_application_test') }}

)

select
    'bureau' as source_table,
    b.sk_id_curr
from {{ ref('stg_bureau') }} as b
left join known_clients as k on b.sk_id_curr = k.sk_id_curr
where k.sk_id_curr is null

union all

select
    'previous_application',
    p.sk_id_curr
from {{ ref('stg_previous_application') }} as p
left join known_clients as k on p.sk_id_curr = k.sk_id_curr
where k.sk_id_curr is null
