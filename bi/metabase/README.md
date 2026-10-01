# Metabase

Metabase reads the dbt marts in **Snowflake dev** (the full dataset). The dashboard is code: `setup.py` creates it through
Metabase's API, so it is reproducible and reviewable.

## Run

```bash
cp .env.example .env     # set MB_ADMIN_EMAIL / MB_ADMIN_PASSWORD (a local account, pick any values) and the SNOWFLAKE_* variables
docker compose -f bi/metabase/docker-compose.yml up -d
python bi/metabase/setup.py          # first start only; safe to rerun (it rebuilds the dashboard)
```

Open http://localhost:3000 and log in with the `MB_ADMIN_*` values. The dashboard is **Credit risk > Credit risk: default story**.

## Why Snowflake dev and not DuckDB

| | Snowflake dev (chosen) | DuckDB sample |
|---|---|---|
| Data | Full dataset: the numbers in the README | 5,000 clients: different (noisier) rates |
| Metabase support | Built-in driver | Community plugin jar, version-coupled to Metabase |
| Credentials | Key-pair, see below | None |
| Cost | A few queries on an XS warehouse that suspends after 60 s; marts are small, auto-run is off | Free |
| Concurrency | Fine | The file allows one writer: Airflow and Metabase would fight for it |

The headline figures only exist on the full data, so Snowflake wins. The cost is negligible (the whole 6-question
dashboard refresh is a few seconds of XS warehouse time).

## Credentials

- The Snowflake **private key is never copied**: the compose file mounts `~/.snowflake` read-only at `/run/snowflake`
  (set `SNOWFLAKE_KEY_DIR` for another folder; the file must be called `rsa_key.p8`), and Metabase is configured with
  that path. No password is used.
- The connection details (account, user, role, key *path*) live in Metabase's own application database, an H2 file in the
  `metabase-data` Docker volume on your machine. Nothing is written to the repo. `docker compose ... down -v` deletes it.
- The Metabase admin is a local account created by `setup.py` from `MB_ADMIN_*` in your git-ignored `.env`.
- Only the `MARTS` schema is exposed to Metabase.

## The six questions

| # | Question | Source | Headline |
|---|---|---|---|
| 1 | Global default rate | `mart_credit_risk` | 8.07% |
| 2 | Default rate by delinquency bucket (lifetime worst DPD) | `mart_credit_risk` | 61-90 at 14.0% but 90+ at 9.5% |
| 3 | Default rate by when the worst delay happened | `fct_installments`, `dim_client` | 25.0% recent vs 8.11% old: explains the 90+ anomaly |
| 4 | Clients with unpaid installments vs the average | `mart_credit_risk` | 18.14% vs 8.07% |
| 5 | Default rate by client seniority | `mart_credit_risk` | falls from 10.4% to 6.3% as tenure grows |
| 6 | Credit exposure by income band | `mart_credit_risk` | dataset currency units, billions |

A text card explains the recency effect. All SQL is in `setup.py` (`QUESTIONS`); the figures match
`docs/headline_metrics.md`.

## Recreate by hand (if you do not want to run the script)

1. Admin > Databases > Add database > Snowflake. Account, user, role `CREDIT_RISK_ENGINEER`, warehouse `CREDIT_RISK_WH`,
   database `CREDIT_RISK_DEV`; authentication "Private key file path": `/run/snowflake/rsa_key.p8`; schema filter:
   only `MARTS`.
2. New > SQL query, paste each query from `QUESTIONS` in `setup.py`, choose the visualization (number for #1, bar for the
   rest), save to a collection "Credit risk".
3. New > Dashboard, add the six questions in the order of the table above plus a text card with the recency note.

## Screenshots to take (owner)

Save in `docs/images/`: `metabase_dashboard.png` (the whole dashboard), `metabase_dpd_recency.png` (questions 2 and 3
side by side).
