# Data quality findings

Findings from running the staging layer and its tests on the **full dataset in Snowflake
(`CREDIT_RISK_DEV`)**. Phase 2 scope: nothing is filtered or corrected beyond the normalisation
rules below; staging stays 1:1 with RAW (row counts match exactly for all seven tables).

**How tests report findings.** Structural tests (unique, not_null, accepted_values, the hard DPD
rule) use `error` severity and fail the build. Tests that describe how the source data really is
(orphan keys, negative amounts, reconciliation) use `severity: warn`: they stay visible in every
`dbt build` but do not block CI, and each one is documented here. Last run on dev: 96 tests,
85 pass, 11 warn, 0 error. On the 5k-client CI sample: 89 pass, 7 warn, 0 error.

## Normalisation rules applied in staging

| # | Finding | Records (dev) | Handling | Why |
|---|---|---:|---|---|
| 1 | `DAYS_EMPLOYED = 365243` in `application_train`. About 1,000 years, clearly a placeholder. 55,352 pensioners and 22 unemployed. | 55,374 (18.0%) | `days_employed` = NULL, `is_days_employed_sentinel` = true | Left as a number it would distort every average and model feature; the flag keeps the original fact. Owner-approved rule. |
| 2 | `ORGANIZATION_TYPE = 'XNA'`. These are exactly the same rows as finding 1 (overlap 55,374 of 55,374). | 55,374 | `organization_type` = NULL, `is_organization_type_xna` = true | XNA is the "not available" code. Owner-approved rule. |
| 3 | `CODE_GENDER = 'XNA'` | 4 | NULL + `is_code_gender_xna` | Same rule. |
| 4 | `XNA` in 10 categorical columns of `previous_application` (contract type 346, cash loan purpose 677,918, payment type 627,384, reject reason 5,244, client type 1,941, goods category 950,809, portfolio 372,230, product type 1,063,666, seller industry 855,720, yield group 517,215) and `pos_cash_balance.name_contract_status` (2) | see left | NULL + `is_<col>_xna` per column | Same rule. |
| 5 | `365243` also appears in five `previous_application` day columns: `days_first_drawing` 934,444; `days_first_due` 40,645; `days_last_due_1st_version` 93,864; `days_last_due` 211,221; `days_termination` 225,913 | see left | NULL + `is_<col>_sentinel` | **Extension of the approved rule**: same value, same meaning. Trivial to revert in `stg_previous_application.sql` if you disagree. |
| 6 | `installments_payments` has no primary key. `(sk_id_prev, num_instalment_version, num_instalment_number)` has 12,951,918 distinct values over 13,605,401 rows: 653,483 extra rows, all from installments paid in several parts. **Exact duplicate rows: 0.** | 13,605,401 | `installment_payment_id` = hash of all 8 business columns. Unique on the full table. | No duplicates to remove. If a future load brought one, the unique test would fail instead of silently dropping it. Owner-approved. |

## Findings surfaced by tests (kept, not hidden)

| # | Test | Finding | Records (dev) | Handling now | Open for phase 3 |
|---|---|---|---:|---|---|
| 7 | `relationships` bureau -> application | Bureau rows whose `sk_id_curr` is not in `application_train` (42,320 distinct clients) | 251,103 | warn | Likely clients of `application_test`, which is out of scope per the brief (hypothesis, not verified). Marts should inner-join on clients. |
| 8 | `relationships` previous_application -> application | Same pattern | 256,513 | warn | Same. |
| 9 | `relationships` bureau_balance -> bureau | `sk_id_bureau` absent from `bureau` (43,041 distinct credits) | 3,120,184 (11.4%) | warn | Source-level gap; balance history without the credit header is unusable for joins. |
| 10 | `relationships` installments -> previous_application | `sk_id_prev` absent from `previous_application` (38,847 distinct loans) | 1,250,826 | warn | Same. |
| 11 | `relationships` pos_cash_balance -> previous_application | as above | 340,561 | warn | Same. |
| 12 | `relationships` credit_card_balance -> previous_application | as above | 1,082,816 | warn | Same. |
| 13 | `assert_installments_reconcile_with_amount` | Sum of payments differs from the amount due by more than 1 cent, per installment. 2,934 underpaid, 179,407 overpaid. Out of 12,951,918 installments (1.4%). | 182,341 installments | warn | Decide how fct_installments treats overpayment (version changes are a candidate explanation, not verified). |
| 14 | `installments` missing payment | `days_entry_payment` and `amt_payment` both NULL (installment due, nothing paid). Not counted in finding 13. | 2,905 | none (valid NULL) | Treat as unpaid in fct_installments. |
| 15 | `assert_installment_payment_not_before_decision` | Payment entered before the application decision date (`days_decision` is the only origination proxy in the source) | 706 | warn | Flag in int layer. |
| 16 | `assert_no_negative_bureau_debt` | `amt_credit_sum_debt` < 0 in 8,418 rows; `amt_credit_sum_limit` < 0 in 351 (8,769 rows in total). Overdue amounts: none negative. | 8,769 | warn | Decide between NULL and keep (negative debt can be a bureau overpayment). |
| 17 | `assert_no_negative_credit_card_amounts` | `amt_balance` < 0: 2,345; `amt_receivable_principal` < 0: 2,428; `amt_drawings_current` < 0: 3; `amt_drawings_atm_current` < 0: 1 (2,432 rows in total) | 2,432 | warn | Negative balance may be a legitimate credit; negative drawings are not. |
| 18 | `assert_bureau_overdue_within_credit_age` | `credit_day_overdue` larger than the age of the credit | 2 | warn | Immaterial; exclude from DPD features. |
| 19 | `assert_days_past_due_are_possible` (error severity) | Negative DPD, or `sk_dpd_def` > `sk_dpd`, in POS, credit card and bureau | 0 | passes | none |

## Observations without a test (for context)

- `days_registration` has 1 fractional value out of 307,511, so it is typed `double`; every other `days_*` column is integral and typed `bigint`.
- `name_family_status = 'Unknown'`: 2 rows. `amt_income_total` max 117,000,000 (3 rows above 10 million).
- `XAP` ("not applicable", not the same as XNA) is frequent: `name_cash_loan_purpose` 922,661 and `code_reject_reason` 1,353,093 rows. **Left untouched**: the approved rule covers XNA only, and XAP is a meaningful business value (for example "no reject reason" on approved loans). Say so if you want it nulled too.
- `bureau`: 29,556 rows with debt above the credit sum, 6,618 `Closed` credits with debt above zero, 20 with `days_enddate_fact` before `days_credit`. No test yet.
- Checked and clean: 0 failed casts across all typed columns (non-null RAW values that became NULL, excluding the flagged ones), 0 values outside {0, 1, Y, N} in the 37 flag columns, 0 duplicate primary keys in the other six tables, `target` only 0/1 (default rate 8.07%).
