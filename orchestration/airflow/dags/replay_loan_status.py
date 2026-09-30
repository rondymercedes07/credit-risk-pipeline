"""SIMULATION of monthly loads into the SCD2 snapshot `snp_loan_status` (manual trigger only).

The dataset is static, so a snapshot run never sees a change. This DAG replays the contract
status history that does exist (pos_cash_balance, months_balance -96..-1) one month at a time:
for each month m in [start_month, end_month] it runs `dbt snapshot` as if a new monthly load had
arrived. dbt_valid_from / dbt_valid_to are therefore the wall-clock times of the replay, not
business dates; the simulated business timeline is the `as_of_month` column.

It drops and rebuilds the snapshot (--fresh), so it is safe to rerun. It needs the staging
models, so run `credit_risk_daily` first on the same target.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PROJECT = "/opt/project"
TARGET = "{{ params.target }}"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=4),
    "pool": "dbt_warehouse",  # same pool as credit_risk_daily: they must not write DuckDB at once
}

with DAG(
    dag_id="replay_loan_status",
    description="Simulated monthly loads of the loan status SCD2 snapshot (manual only)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "start_month": Param(-12, type="integer", minimum=-96, maximum=-1),
        "end_month": Param(-1, type="integer", minimum=-96, maximum=-1),
        "target": Param("ci", enum=["ci", "dev", "prod"], description="dbt / loader target"),
    },
    tags=["credit-risk", "dbt", "simulation"],
    doc_md=__doc__,
) as dag:
    replay_snapshot = BashOperator(
        task_id="replay_snapshot",
        bash_command=(
            f"python scripts/replay_loan_status.py --target {TARGET} "
            "--start {{ params.start_month }} --end {{ params.end_month }} --fresh"
        ),
        cwd=PROJECT,
    )
    test_snapshot = BashOperator(
        task_id="test_snapshot",
        bash_command=f"dbt test --target {TARGET} --select snp_loan_status+",
        cwd=f"{PROJECT}/dbt",
    )
    verify_snapshot = BashOperator(
        task_id="verify_snapshot",
        bash_command=f"python scripts/verify_pipeline.py --target {TARGET} --with-snapshot",
        cwd=PROJECT,
    )

    replay_snapshot >> test_snapshot >> verify_snapshot
