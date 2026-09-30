"""Daily credit-risk pipeline: download -> load RAW -> dbt build -> snapshot replay -> verify.

Every step shells out to the same commands you would run by hand, so the DAG adds scheduling,
dependencies, retries and observability but no logic of its own. Python and dbt live in a
separate virtualenv (/opt/venv) because dbt's dependency pins conflict with Airflow's.

The `target` param selects the warehouse: `ci` (default) runs on DuckDB with the committed
sample and needs no credentials; `dev` / `prod` run on Snowflake with the full dataset.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task
from airflow.task.trigger_rule import TriggerRule

PROJECT = "/opt/project"
TARGET = "{{ params.target }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="credit_risk_daily",
    description="Home Credit pipeline: Kaggle -> RAW -> dbt (staging, marts, tests) -> checks",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # runs share one warehouse (and one DuckDB file on ci)
    default_args=default_args,
    params={
        "target": Param("ci", enum=["ci", "dev", "prod"], description="dbt / loader target"),
        "replay_start_month": Param(
            -12,
            type="integer",
            minimum=-96,
            maximum=-1,
            description="First simulated monthly load of the SCD2 snapshot (-96 = full history)",
        ),
    },
    tags=["credit-risk", "dbt"],
    doc_md=__doc__,
) as dag:

    @task.branch
    def needs_download(params=None) -> str:
        # The ci sample is committed to the repo; only full-dataset targets hit Kaggle.
        return "skip_download" if params["target"] == "ci" else "download_data"

    branch = needs_download()
    skip_download = EmptyOperator(task_id="skip_download")
    download_data = BashOperator(
        task_id="download_data",
        bash_command="python scripts/download_data.py",
        cwd=PROJECT,
    )
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"python scripts/load_raw.py --target {TARGET}",
        cwd=PROJECT,
        # One of the two upstream branches is always skipped; that must not skip the load.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    dbt_deps = BashOperator(task_id="dbt_deps", bash_command="dbt deps", cwd=f"{PROJECT}/dbt")

    # The snapshot and the tests that read it cannot run before the replay has populated it,
    # so they are carved out of the main build and run in the two tasks below.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build --target {TARGET} --exclude snp_loan_status+",
        cwd=f"{PROJECT}/dbt",
    )
    replay_snapshot = BashOperator(
        task_id="replay_snapshot",
        bash_command=(
            f"python scripts/replay_loan_status.py --target {TARGET} "
            "--start {{ params.replay_start_month }} --end -1 --fresh"
        ),
        cwd=PROJECT,
    )
    test_snapshot = BashOperator(
        task_id="test_snapshot",
        bash_command=f"dbt test --target {TARGET} --select snp_loan_status+",
        cwd=f"{PROJECT}/dbt",
    )
    verify = BashOperator(
        task_id="verify",
        bash_command=f"python scripts/verify_pipeline.py --target {TARGET}",
        cwd=PROJECT,
    )

    branch >> [skip_download, download_data] >> load_raw
    load_raw >> dbt_deps >> dbt_build >> replay_snapshot >> test_snapshot >> verify
