{{ config(severity='warn') }}

-- Business rule: what was paid for an installment should equal the amount due. Checked at the
-- installment grain (amount due = sum over versions, payments once), which removes the version
-- artefact of the source: 182,341 "mismatches" at the version grain become 3,453 real ones
-- (3,234 underpaid + 219 overpaid; docs/data_quality_findings.md, finding 13). Unpaid installments are a
-- separate finding and are not counted here. Tolerance: 1 cent.
select
    installment_id,
    sk_id_prev,
    num_instalment_number,
    amt_due,
    amt_paid
from {{ ref('int_installment_schedule') }}
where is_underpaid or is_overpaid
