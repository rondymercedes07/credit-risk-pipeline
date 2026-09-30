# Modeling decisions (phase 3)

Status: **proposal, pending owner approval** before any intermediate or mart SQL is written.
Numbers quoted here were measured on the full dataset (local DuckDB over the Kaggle CSVs, no
warehouse credits) and are reproduced by the dbt tests once the models exist.

## 1. Universe

| Layer | Clients included | Why |
|---|---|---|
| `dim_client` | train **and** test, column `client_source` in (`train`, `test`) | Complete client list; `is_default` is NULL for test. |
| `fct_loans`, `fct_installments`, all risk marts | **train only** | Only train has TARGET. Adding unlabelled clients would inflate exposure and loan counts without ever entering a default rate. |

Every fact carries `sk_id_curr` with a `relationships` test to `dim_client`. Two things to know:
a test client exists in `dim_client` with no facts (harmless), and any average over `dim_client`
must filter `client_source = 'train'`; `mart_credit_risk` does, and a test asserts it.
I see no blocking problem with the proposal.

**Orphan loans.** 48,799 loans have payments, POS or card history but no row in
`previous_application` (see `data_quality_findings.md`, cause unknown). `fct_installments` keeps
their installments with `has_loan_record = false`, because each row carries its own `sk_id_curr`
and client-level DPD should not lose them. `fct_loans` cannot contain them (no decision date,
type or status). Relationship tests to `fct_loans` are scoped to `has_loan_record`.

## 2. Grain of every model

| Model | Layer | One row per | Key | Materialization |
|---|---|---|---|---|
| `int_installment_schedule` | int | scheduled installment of a loan | `(sk_id_prev, num_instalment_number)` | view |
| `int_loan_payment_performance` | int | previous loan that has installments | `sk_id_prev` | view |
| `int_pos_cash_loan` | int | POS/cash loan | `sk_id_prev` | view |
| `int_credit_card_loan` | int | credit card contract | `sk_id_prev` | view |
| `int_bureau_client` | int | client with bureau credits | `sk_id_curr` | view |
| `int_client_credit_history` | int | client (train and test) | `sk_id_curr` | view |
| `dim_client` | mart | client (train and test) | `sk_id_curr` | table |
| `fct_loans` | mart | previous Home Credit application of a train client | `sk_id_prev` | table |
| `fct_installments` | mart | scheduled installment of a train client | `installment_id` (hash of `sk_id_prev`, `num_instalment_number`) | **incremental (merge)** |
| `mart_credit_risk` | mart | (segment dimension, segment value) | `(dimension, dimension_value)` | table |
| `mart_vintage_curves` | mart | (loan cohort, months on book) | `(cohort, months_on_book)` | table |
| `snp_loan_status` | snapshot | version of a POS/cash loan status | `(sk_id_prev, dbt_valid_from)` | SCD2 snapshot |

`mart_vintage_curves` is a second risk mart: its grain (cohort x month on book) cannot share a
table with segment rates without a column that is NULL half the time. The brief lists one
`mart_credit_risk`; say so if you prefer a single long table and I will merge them.

**Installment grain, and why it is not the source grain.** `installments_payments` has one row
per payment *and* per version. Verified: 89,897 installment numbers exist in 2 or 3 versions;
the amount due is split across versions (11,667.195 + 32.805 = 11,700) while the same payment row
is repeated in each version. So an installment is `(sk_id_prev, num_instalment_number)` with
`amt_due = sum of amt_instalment over versions` and payments taken once (from the lowest
version). With that rule 12,855,651 of 12,861,994 installments reconcile (99.95%); the
"179,407 overpayments" of the version grain become 219. The due date is `days_instalment` of the
lowest version (303 numbers have versions with different due dates; the first version wins).

## 3. Definition of default

`is_default` = `TARGET` of `application_train`: the client had payment difficulties on the
**current application** (the one the dataset is about). It is a **client / application flag**,
not a loan-level flag: it says nothing about which previous loan went bad. Consequences:

- It lives on `dim_client` only. It is deliberately **not** copied to `fct_loans`, so nobody
  computes a "loan default rate" that is really a client rate counted once per loan.
- Default rate = `sum(is_default) / count(clients)` over train clients, always at client grain.
- Rate of the train set is 8.07%. It is the rate of the labelled sample, not of the portfolio.

## 4. Days past due (DPD) per installment

All day columns are days relative to the current application (negative = past), so
`days_entry_payment - days_instalment` is a difference of two dates on the same axis.

- `days_past_due_raw = last_payment_day - due_day` (negative = paid early).
- `dpd = greatest(days_past_due_raw, 0)`. Early payment is not negative delinquency.
- **Partial payments** (several rows per installment; about 641k installment versions have 2 or more):
  `last_payment_day` is the date of the last payment row, i.e. when the installment stopped being
  open. `amt_paid` is the sum. Flags: `is_underpaid` (paid < due - 0.01), `is_overpaid`
  (paid > due + 0.01), kept separately so a late-but-complete installment and a short-paid one
  can be told apart. Underpaid installments (3,234; median shortfall 37%) keep the DPD of their last
  payment; the shortfall is carried in `amt_shortfall`, not converted into extra delinquency.
- **Installments without payment** (2,890, both payment fields NULL): `dpd` is NULL,
  `is_unpaid = true`. No "days overdue so far" is invented, because the source has no
  observation date that I could verify. They are excluded from buckets and counted in a
  separate column, so they are visible and not silently treated as on time.
- Payment before the decision date (706 rows, finding 15) is flagged `is_paid_before_decision`
  and kept.

## 5. Delinquency buckets

`0` (on time or early), `1-30`, `31-60`, `61-90`, `90+`. Computed from `dpd`.

**Both loan level and client level, for different questions:**

- **Per loan** (`fct_loans.max_dpd`, `dpd_bucket`): portfolio view and vintage, where the unit is
  the loan.
- **Per client** (`int_client_credit_history.max_dpd`, `worst_dpd_bucket`): the default flag is
  per client, so the mart that relates delinquency to default must use the client's worst DPD
  across all their installments (orphan loans included), else one client would count once per
  loan. Clients with no installment history get `no_history`.

Maximum DPD is deliberately "worst ever", not "latest": it is a risk signal, and the dataset has
no observation date to define "latest".

**Observed, not hidden.** Default rate by client worst bucket on the full data: 0 = 6.80%,
1-30 = 9.23%, 31-60 = 10.84%, 61-90 = 14.39%, **90+ = 9.63%**, no history = 5.98%. The rate is
monotone up to 61-90 and then drops at 90+. I have no verified explanation (catch-up payments of
old arrears are a guess) and will report it as an observation in the mart description and the
README, not smooth it away.

## 6. Cohorts and vintage without absolute dates

The dataset has **no calendar dates**: every time value is relative to each client's own current
application. A calendar cohort ("loans from 2015-Q3") cannot be built and I will not fabricate one.
Two honest alternatives, both anchored on relative months:

1. **Default rate by tenure cohort** (client level, in `mart_credit_risk`). Cohort = 12-month block
   of the time between the client's **first approved Home Credit loan** and the current application
   (`0-11m`, `12-23m`, ..., `no_prior_loan`). One cohort per client, so the rate stays at client
   grain. Measured: the default rate falls from 10.4% (first loan less than 1 year before the application)
   to 6.3% (7 to 8 years) and 6.1% without prior loans.
2. **Vintage curves** (loan level, `mart_vintage_curves`). Cohort = 12-month block of
   `days_decision` of the previous loan (months before the current application). Months on book
   (MOB) = `months_balance - first observed month` in POS_CASH. Verified: the first POS month
   is within 2 months of the decision for 99.2% of loans (891,838 of 898,903). Metric = cumulative
   share of loans that reached 30+ DPD (`sk_dpd_def >= 30`) by MOB m, denominator = loans
   observable at m (young cohorts have shorter curves, the normal right-censoring of vintages).
   Loans first seen at month -96, the start of the window, are left-censored and excluded.

**Trade-off (what this does not give you).** A "cohort" here is *loan age at the client's current
application*, not a calendar origination vintage: two loans in the same cohort were originated at
different real dates, so macro effects (a crisis year) cannot be separated from loan age, and the
cohort default rate mixes that. In exchange both views are computable, honest about their anchor, and
the curve shape (how fast delinquency accumulates with loan age) is valid because MOB is relative to
the loan itself. The vintage covers POS and cash loans only (credit cards have no fixed origination
and term). The README will state the anchor explicitly.

## 7. SCD2 snapshot (proposal, needs your OK)

The data is static, so a snapshot run against it never detects a change. A real SCD2 cannot be
shown without pretending. Proposal: **replay history as simulated incremental loads, and say so.**

- Status history that really exists: `pos_cash_balance.name_contract_status` per loan per
  `months_balance` (96 months: Signed, Active, Completed, Demand, Returned to the store, ...).
- `snap_loan_status_source` (a model) returns each loan's latest status with
  `months_balance <= var('as_of_month')`; loans not yet observed at that month are absent.
- `snp_loan_status`: dbt snapshot, `strategy='check'`, `check_cols=['name_contract_status']`,
  `unique_key='sk_id_prev'`. The column `as_of_month` (the simulated month) is stored but not
  compared.
- `scripts/replay_loan_status.py` runs `dbt snapshot --vars '{as_of_month: m}'` for m = -96 ... -1
  on CI (DuckDB sample, full replay) and for a limited window on dev (-12 ... -1, to keep credits
  low).
- **What is real and what is simulated.** `dbt_valid_from` / `dbt_valid_to` are the real
  wall-clock times of each replay run (minutes apart), *not* business dates. The business timeline
  is `as_of_month`. The docs and the snapshot description will say "simulated monthly loads".
- Tests: exactly one current row per loan; no overlapping validity intervals; number of versions
  per loan equals the number of status changes counted independently from the POS history;
  re-running the same month adds 0 rows (idempotency).

## 8. `fct_installments` incremental strategy

- **Merge on `installment_id`**, not append: an installment already loaded can change later (a
  payment arrives for a row that had none), and append would keep the stale row next to the new one.
  Not `insert_overwrite`: there are no date partitions.
- **Watermark** `loaded_at > max(loaded_at)` of the target (RAW audit column), then re-aggregate
  *all* payment rows of the affected installments (a new partial payment must be added to the old
  ones, not aggregated alone).
- **Honest limit.** `scripts/load_raw.py` reloads each table in full, so every `_loaded_at`
  changes on every load and the watermark then selects everything. The strategy pays off once the
  loader appends daily deltas (Airflow, phase 4); until then a run is a full, idempotent re-merge.
- **Test in dev, three runs:** (1) first build, (2) immediately again: 0 rows processed,
  (3) after re-running the loader: full re-merge, row count and unique test unchanged. Row counts
  and `_loaded_at` bounds are logged for each run.

## 9. Segments, exposure and other choices

- `mart_credit_risk` dimensions: `name_contract_type`, `code_gender`, `age_band` (from
  `days_birth`), `income_quintile`, `name_education_type`, `name_income_type`,
  `region_rating_client`, `tenure_cohort`, `worst_dpd_bucket`. NULLs become `unknown`, so every
  dimension partitions the train clients exactly once.
- **Exposure** = `amt_credit` of the current application (the credit being decided), plus
  `bureau_active_debt` (sum of `amt_credit_sum_debt` of active bureau credits, negative debts
  excluded, finding 16). Both are reported as separate columns; nothing is added across sources.
- Metrics per row: `n_clients`, `n_defaults`, `default_rate`, exposure sums, `share_of_clients`.
  Business tests: per dimension, clients and exposure sum to the train total; `n_defaults <= n_clients`.
- The moved reconciliation test (`assert_installments_reconcile_with_amount` at the installment
  grain) stays `warn`; the 219 + 3,234 real cases remain visible.

## 10. Decisions I need from you

1. Facts restricted to the train universe (section 1).
2. Two risk marts instead of one (section 2).
3. Snapshot as simulated monthly replay on POS_CASH status (section 7), full on CI, last 12 months on dev.
4. Unpaid installments get `dpd = NULL` and a flag, no invented overdue days (section 4).
5. Client-level buckets use worst-ever DPD from installments; POS `sk_dpd_def` and bureau overdue
   stay as separate columns, not merged into one number.
