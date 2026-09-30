# Credit Risk Pipeline — Project Brief

## Goal
Public portfolio project on GitHub: a production-grade credit-risk data pipeline, built to demonstrate for a "Data Engineer, Credit Risk" application (see ../postulacion/vacante.md if available) hands-on skill with Snowflake, dbt, orchestration, data quality and CI. All code, README, comments and commits in English.

## Data
- Dataset: Home Credit Default Risk (Kaggle). Tables: application_train, bureau, bureau_balance, previous_application, installments_payments, POS_CASH_balance, credit_card_balance.
- Raw CSVs are never committed. scripts/download_data.py downloads via Kaggle API; scripts/load_raw.py loads to the RAW layer.
- data/sample/: deterministic sample (5,000 clients, seed 42, all related records) used for CI.

## Warehouse (two targets)
- dev/prod: Snowflake, key-pair auth, credentials only via environment variables. scripts/snowflake_setup.sql creates database, schemas (RAW, STAGING, MARTS), XS warehouse with 60s auto-suspend, resource monitor and dedicated role.
- ci: DuckDB on the sample. The dbt project must run identically on both; platform-specific SQL goes in macros with adapter dispatch.
- RAW stores everything as VARCHAR; typing happens in staging.

## dbt
- Layers: staging (stg_*, 1:1 with sources, clear snake_case names, correct types, no business logic) -> intermediate (int_*, joins and aggregations) -> marts (fct_loans, fct_installments, dim_client, mart_credit_risk).
- mart_credit_risk: default rate by origination cohort, delinquency buckets DPD 0/30/60/90+, vintage analysis, aggregated exposure by segment.
- At least one incremental model (fct_installments) with the strategy justified in a comment.
- One SCD2 snapshot of loan status.
- sources.yml with descriptions and freshness where applicable.
- Tests: unique/not_null on all keys, relationships between facts and dimensions, accepted_values on categoricals, and custom business tests in tests/ (payment before origination, negative balances, installments not reconciling with amount, impossible days-past-due). Use dbt-utils and dbt-expectations.
- Every test that fails on real data is a finding, not something to hide. Document in docs/data_quality_findings.md what was found, how many records are affected, how it was handled (filter, flag, fix) and why.
- Descriptions on all models and on all mart columns.

## Orchestration
- Airflow (named in the job posting): download -> load RAW -> dbt build -> verification, with explicit dependencies, retries and a daily schedule.
- Must start locally with a single documented command.

## CI/CD
- GitHub Actions on every PR: install deps, sqlfluff lint (correct dialect, dbt templater), dbt build --target ci. Fails if any test fails.
- Separate workflow publishing dbt docs to GitHub Pages on every push to main.
- pre-commit with sqlfluff and ruff. .gitattributes with `* text=auto eol=lf`.

## BI
- Metabase via Docker pointing at the marts (named in the job posting).
- dashboard/ folder for the owner's Power BI .pbix and screenshots; README section with placeholders.

## Final deliverables
- English README: problem solved, architecture diagram (Mermaid), how to run, design decisions with trade-offs, data quality findings summary, dbt docs link, dashboard screenshots.
- Clean conventional commit history (feat:, fix:, docs:, test:, chore:, data:), in logical steps.

## Phases (stop after each one and report)
1. Structure + download + load (DONE)
2. Staging + tests (DONE)
3. Intermediate + marts (DONE)
4. Orchestration (Airflow) (DONE)
5. CI + dbt docs + README + Metabase

## Rules
- At the end of each phase, explain non-obvious decisions in 5-10 lines. The owner will defend this project in a technical interview and must understand every decision.
- No credentials in the repo. .env.example and .gitignore kept correct.
- Never push without explicit approval.
- If the dataset contradicts an assumption, ask before inventing a business rule.
- Always use .venv\Scripts\python (Python 3.12), never the system Python.
