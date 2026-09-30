"""Post-run verification of the warehouse: the last task of the Airflow DAG.

dbt tests already validate the models; this checks what they cannot see from inside a run:
that the layers are populated and reconcile with each other end to end (RAW -> staging -> marts).
Exits non-zero listing every failed check, so the orchestrator marks the run as failed.

Usage:
    python scripts/verify_pipeline.py --target ci
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from credit_risk_pipeline import config

MARTS = ("fct_loans", "fct_installments", "dim_client", "mart_credit_risk", "mart_vintage_curves")


def make_scalar(target: str) -> Callable[[str], int]:
    """Return a function that runs a single-value query on the target warehouse."""
    if target == "ci":
        import duckdb

        con = duckdb.connect(str(config.duckdb_path()), read_only=True)
    else:
        import snowflake.connector

        con = snowflake.connector.connect(**config.snowflake_connect_kwargs(target))

    def scalar(sql: str) -> int:
        cur = con.cursor() if target != "ci" else con
        return int(cur.execute(sql).fetchone()[0])

    return scalar


def run_checks(scalar: Callable[[str], int]) -> list[str]:
    """Return one message per failed check (empty list = everything reconciles)."""
    failures: list[str] = []

    for mart in MARTS:
        if scalar(f"select count(*) from marts.{mart}") == 0:
            failures.append(f"marts.{mart} is empty")

    raw_train = scalar("select count(*) from raw.application_train")
    if raw_train == 0:
        failures.append("raw.application_train is empty")
    stg_train = scalar("select count(*) from staging.stg_application_train")
    if stg_train != raw_train:
        failures.append(f"staging lost or duplicated clients: raw={raw_train} staging={stg_train}")

    # Any single dimension of mart_credit_risk partitions the train clients exactly once.
    mart_clients = scalar(
        "select sum(n_clients) from marts.mart_credit_risk where dimension = 'code_gender'"
    )
    if mart_clients != raw_train:
        failures.append(f"mart_credit_risk covers {mart_clients} clients, raw has {raw_train}")

    if scalar("select count(*) from marts.snp_loan_status") == 0:
        failures.append("marts.snp_loan_status is empty (snapshot replay did not run)")

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target", choices=config.TARGETS, required=True)
    args = parser.parse_args()

    failures = run_checks(make_scalar(args.target))
    if failures:
        sys.exit("Verification failed:\n - " + "\n - ".join(failures))
    print(f"Verification passed on target {args.target}.")


if __name__ == "__main__":
    main()
