-- A client cannot be in both the labelled (train) and the unlabelled (test) application sets:
-- dim_client derives its client_source column from this.
select tr.sk_id_curr
from {{ ref('stg_application_train') }} as tr
inner join {{ ref('stg_application_test') }} as te on tr.sk_id_curr = te.sk_id_curr
