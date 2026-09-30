# Data quality findings

Findings from running the staging layer and its tests on the **full dataset in Snowflake
(`CREDIT_RISK_DEV`)**. Phase 2 scope: nothing is filtered or corrected beyond the normalisation
rules below; staging stays 1:1 with RAW (row counts match exactly for all seven tables).

**How tests report findings.** Structural tests (unique, not_null, accepted_values, the hard DPD
rule) use `error` severity and fail the build. Tests that describe how the source data really is
(orphan keys, negative amounts, reconciliation) use `severity: warn`: they stay visible in every
`dbt build` but do not block CI, and each one is documented here. Phase 2 run on dev: 96 tests,
85 pass, 11 warn, 0 error (counts for the current state are in the phase 3 summary).

## Normalisation rules applied in staging

| # | Finding | Records (dev) | Handling | Why |
|---|---|---:|---|---|
| 1 | `DAYS_EMPLOYED = 365243` in `application_train`. About 1,000 years, clearly a placeholder. 55,352 pensioners and 22 unemployed. | 55,374 (18.0%) | `days_employed` = NULL, `is_days_employed_sentinel` = true | Left as a number it would distort every average and model feature; the flag keeps the original fact. Owner-approved rule. |
| 2 | `ORGANIZATION_TYPE = 'XNA'`. These are exactly the same rows as finding 1 (overlap 55,374 of 55,374). | 55,374 | `organization_type` = NULL, `is_organization_type_xna` = true | XNA is the "not available" code. Owner-approved rule. |
| 3 | `CODE_GENDER = 'XNA'` | 4 | NULL + `is_code_gender_xna` | Same rule. |
| 4 | `XNA` in 10 categorical columns of `previous_application` (contract type 346, cash loan purpose 677,918, payment type 627,384, reject reason 5,244, client type 1,941, goods category 950,809, portfolio 372,230, product type 1,063,666, seller industry 855,720, yield group 517,215) and `pos_cash_balance.name_contract_status` (2) | see left | NULL + `is_<col>_xna` per column | Same rule. |
| 5 | `365243` also appears in five `previous_application` day columns: `days_first_drawing` 934,444; `days_first_due` 40,645; `days_last_due_1st_version` 93,864; `days_last_due` 211,221; `days_termination` 225,913 | see left | NULL + `is_<col>_sentinel` | Extension of the 365243 rule to these five columns: same value, same meaning. Owner-approved; trivial to revert in `stg_previous_application.sql`. |
| 6 | `installments_payments` has no primary key. `(sk_id_prev, num_instalment_version, num_instalment_number)` has 12,951,918 distinct values over 13,605,401 rows: 653,483 extra rows, all from installments paid in several parts. **Exact duplicate rows: 0.** | 13,605,401 | `installment_payment_id` = hash of all 8 business columns. Unique on the full table. | No duplicates to remove. If a future load brought one, the unique test would fail instead of silently dropping it. Owner-approved. |

## Findings surfaced by tests (kept, not hidden)

| # | Test | Finding | Records (dev) | Handling now | Open for phase 3 |
|---|---|---|---:|---|---|
| 7 | `assert_client_keys_exist_in_application` (was a `relationships` warn) | Bureau rows whose `sk_id_curr` is not in `application_train`: 251,103 rows, 42,320 distinct clients. **Root cause confirmed: 100% (251,103 of 251,103) are clients of `application_test`.** 0 unexplained. | 251,103 | Explained. The warn test was replaced by one with `error` severity that checks train ∪ test; it passes. | Marts do not need these rows for default analysis (no TARGET). |
| 8 | same | Same for `previous_application`: 256,513 rows, 47,800 distinct clients. **100% are clients of `application_test`.** 0 unexplained. | 256,513 | Same. | Same. |
| 9 | `relationships` bureau_balance -> bureau | `sk_id_bureau` absent from `bureau`: 43,041 distinct credits (of 817,395 in bureau_balance). **Root cause not found.** `bureau_balance` has no client key, so it cannot be tied to `application_test`. The missing ids lie inside the id range of `bureau` (5,001,709 to 6,842,857 vs 5,000,000 to 6,843,457), i.e. gaps, not ids beyond the file. | 3,120,184 (11.4%) | warn | Unexplained residual. Balance history without the credit header cannot be joined to a client, so it is excluded from client-level features. |
| 10 | `relationships` installments -> previous_application | `sk_id_prev` absent from `previous_application`: 38,847 distinct loans. **Hypothesis "test clients" rejected:** orphan rows of test clients are 18.5% of the total, but test clients are 14.8% of all installment rows, and the orphan rate is similar for train and test clients (11.5% vs 8.8%). All orphan loans belong to clients that exist in train or test. | 1,250,826 | warn | Unexplained residual (see note below). |
| 11 | `relationships` pos_cash_balance -> previous_application | 37,422 distinct loans. Orphan rate 3.4% for train clients and 3.4% for test clients: **test is not the cause.** | 340,561 | warn | Unexplained residual. |
| 12 | `relationships` credit_card_balance -> previous_application | 11,372 distinct loans. Orphan rate 27.0% for train clients, 34.3% for test clients: **test is not the cause.** | 1,082,816 | warn | Unexplained residual. |

**Orphan loans (rows 10 to 12), what is known and what is not.** 48,799 distinct loans appear in `installments_payments`, `pos_cash_balance` or `credit_card_balance` without a row in `previous_application`. Verified facts: (a) they are the same loans across tables (38,842 of the 38,847 orphan loans of `installments_payments` are also orphaned in `pos_cash_balance` or `credit_card_balance`; 28,647 of them in POS, 10,195 in credit card), so the gap is on the `previous_application` side and not random per table; (b) their ids lie inside the id range of `previous_application`; (c) every one of their clients exists in train or test. The reason they are missing from `previous_application` is **not known** and is not guessed here. Consequence for modelling: these rows have no decision date, contract type or status, so loan-level marts exclude them and report the count; client-level DPD is still computed from `installments_payments`, which carries `sk_id_curr` itself.

**Orphan summary (full dataset, dev).**

| Relation | Orphan rows | Explained by `application_test` | Unexplained |
|---|---:|---:|---:|
| bureau -> application (client) | 251,103 | 251,103 (100%) | 0 |
| previous_application -> application (client) | 256,513 | 256,513 (100%) | 0 |
| installments -> previous_application (loan) | 1,250,826 | 0 (test is not the cause) | 1,250,826 |
| pos_cash -> previous_application (loan) | 340,561 | 0 | 340,561 |
| credit_card -> previous_application (loan) | 1,082,816 | 0 | 1,082,816 |
| bureau_balance -> bureau (credit) | 3,120,184 | 0 (no client key to test it) | 3,120,184 |

"Explained" means the client of the orphan row is in `application_test`. For the loan relations that is **not** a cause even when the client is a test client (231,455 of the installment orphans belong to test clients, in line with their 14.8% share of all rows), so they are counted as unexplained.

| 13 | `assert_installments_reconcile_with_amount` | Sum of payments differs from the amount due by more than 1 cent, per `(sk_id_prev, num_instalment_version, num_instalment_number)`: 2,934 underpaid, 179,407 overpaid, out of 12,951,918 (1.4%). **Root cause found: installment versions.** 89,897 installment numbers exist in 2 or 3 versions. The amount due is split across the versions (for example 11,667.195 + 32.805 = 11,700) while the same payment row is repeated in every version (11,700 in each). Compared per version the payment looks like an overpayment. At the right grain, `(sk_id_prev, num_instalment_number)` with amount due = sum over versions and payments taken once (from the lowest version): 12,861,994 installments, **12,855,651 reconcile, 219 overpaid, 3,234 underpaid, 2,890 unpaid**. The payment sets of the versions are identical for 89,594 of the 89,897 multi-version numbers. | 182,341 at the version grain; 3,453 at the installment grain | warn (version grain) | `int_installment_schedule` works at the installment grain; the test moves there. The 3,234 underpaid have a median shortfall of 37% of the amount due (1,430 are short by more than 50%). The 219 overpaid stay as a warn. |
| 14 | `installments` missing payment | `days_entry_payment` and `amt_payment` both NULL (installment due, nothing paid). Not counted in finding 13. | 2,905 | none (valid NULL) | Treat as unpaid in fct_installments. |
| 15 | `assert_installment_payment_not_before_decision` | Payment entered before the application decision date (`days_decision` is the only origination proxy in the source) | 706 | warn | Flag in int layer. |
| 16 | `assert_no_negative_bureau_debt` | `amt_credit_sum_debt` < 0 in 8,418 rows; `amt_credit_sum_limit` < 0 in 351 (8,769 rows in total). Overdue amounts: none negative. | 8,769 | warn | Decide between NULL and keep (negative debt can be a bureau overpayment). |
| 17 | `assert_no_negative_credit_card_amounts` | `amt_balance` < 0: 2,345; `amt_receivable_principal` < 0: 2,428; `amt_drawings_current` < 0: 3; `amt_drawings_atm_current` < 0: 1 (2,432 rows in total) | 2,432 | warn | Negative balance may be a legitimate credit; negative drawings are not. |
| 18 | `assert_bureau_overdue_within_credit_age` | `credit_day_overdue` larger than the age of the credit | 2 | warn | Immaterial; exclude from DPD features. |
| 19 | `assert_days_past_due_are_possible` (error severity) | Negative DPD, or `sk_dpd_def` > `sk_dpd`, in POS, credit card and bureau | 0 | passes | none |

## Investigation: default rate by worst DPD is not monotone at 90+

Finding surfaced while building `mart_credit_risk`. Client-level worst DPD is taken from the paid
installments of all the client's loans (`int_installment_schedule`); the default rate is the
`TARGET` rate of the train clients. Computed on the full dataset (307,511 clients).

**a) Sample size and confidence intervals (Wilson 95%).**

| Worst DPD bucket | Clients | Defaults | Default rate | 95% CI |
|---|---:|---:|---:|---|
| 0 | 136,590 | 9,292 | 6.80% | 6.67 to 6.94% |
| 1-30 | 136,109 | 12,560 | 9.23% | 9.08 to 9.38% |
| 31-60 | 10,253 | 1,111 | 10.84% | 10.25 to 11.45% |
| 61-90 | 1,598 | 230 | 14.39% | 12.76 to 16.20% |
| 90+ | 7,093 | 683 | 9.63% | 8.96 to 10.34% |
| no paid-installment history | 15,868 | 949 | 5.98% | 5.62 to 6.36% |

The intervals of 61-90 and 90+ **do not overlap**: the drop is not sampling noise.

**b) Recency of the worst DPD** (due day of the installment with the worst DPD; "last 12m" = due in the
365 days before the current application).

| Bucket | Worst DPD in last 12m | 12 to 24m ago | More than 24m ago |
|---|---|---|---|
| 0 | 6.73% (96,671) | 6.81% (19,173) | 7.12% (20,738) |
| 1-30 | 12.81% (34,406) | 10.14% (24,794) | 7.33% (76,909) |
| 31-60 | 21.86% (883) | 16.64% (1,058) | 8.93% (8,312) |
| 61-90 | 21.23% (146) | 23.18% (302) | 11.22% (1,150) |
| 90+ | **25.00% (164)** | **21.14% (615)** | **8.11% (6,314)** |

(Clients in brackets.) Recent 90+ has the **highest** default rate of all (25.0%); old 90+ is at
the level of the overall population (8.07%). Old events dominate the bucket: 6,314 of 7,093 clients
(89%) have their worst DPD more than 24 months before the application. The same gradient (older
worst DPD, lower default) appears in every bucket, not only in 90+. Two further cuts, for completeness:
within 90+, clients with 2 to 3 installments over 90 days default at 16.5% and with one at 8.7%;
clients whose worst DPD is above 365 days (2,825) default at 8.0%, the base rate.

**c) What the data supports.**

- *Supported*: the non-monotone shape is a **recency effect**. The 90+ bucket is mostly old
  delinquency and old delinquency carries little information about the current application; recent
  90+ is the riskiest group. Lifetime worst DPD mixes the two, which is why `mart_credit_risk` also
  has the dimension `worst_dpd_bucket_12m`.
- *Consistent but not testable*: selection bias (clients who fell behind long ago, recovered and came
  back to apply are a filtered population). The data fits that story, but it cannot be told apart
  from a plain "old delinquency fades" effect, because nothing in the dataset records why a client
  is applying again. It stays a hypothesis.
- *Not supported*: sample size (intervals do not overlap); a single extreme-DPD artefact (clients above 365 days
  are 40% of 90+ and sit at the base rate, but the shape also holds for buckets 1-30 to 61-90, which have no such extremes).
- No verified explanation for why old delinquency is so weakly predictive. Stated as such.

## Investigation: clients with unpaid installments

2,890 installments of all clients have no payment at all (finding 14); 1,075 train clients have at least one.

| Group | Clients | Default rate | 95% CI |
|---|---:|---:|---|
| At least one unpaid installment | 1,075 | **18.14%** | 15.95 to 20.56% |
| None | 306,436 | 8.04% | 7.94 to 8.13% |

Clearly higher (intervals far apart). Decision: a separate **`unpaid`** bucket in the `dpd_bucket`
dimension of `mart_credit_risk` (precedence over the paid-installment bucket), not mixed into 90+
because the overdue days of an unpaid installment are unknown. Clients also carry
`n_unpaid_installments` and `has_unpaid_installment`. Among these clients the default rate is
13.3% to 26.3% depending on their DPD on paid installments, so the flag is not just a proxy of the bucket.

## Observations without a test (for context)

- `days_registration` has 1 fractional value out of 307,511, so it is typed `double`; every other `days_*` column is integral and typed `bigint`.
- `name_family_status = 'Unknown'`: 2 rows. `amt_income_total` max 117,000,000 (3 rows above 10 million).
- **XAP is not XNA.** `XAP` means "not applicable": the question did not apply to that record, so it is a legitimate value and is **kept as is**. `XNA` means "not available": the information exists in principle but is missing, so it is a missing value and is **nulled and flagged** (rules 2 to 4). Frequency of XAP: `name_cash_loan_purpose` 922,661 and `code_reject_reason` 1,353,093 rows. Example: a loan that was approved has no reject reason, so `code_reject_reason = 'XAP'` is correct, while an XNA in `name_contract_type` is a gap. Owner-approved.
- `bureau`: 29,556 rows with debt above the credit sum, 6,618 `Closed` credits with debt above zero, 20 with `days_enddate_fact` before `days_credit`. No test yet.
- Checked and clean: 0 failed casts across all typed columns (non-null RAW values that became NULL, excluding the flagged ones), 0 values outside {0, 1, Y, N} in the 37 flag columns, 0 duplicate primary keys in the other six tables, `target` only 0/1 (default rate 8.07%).
