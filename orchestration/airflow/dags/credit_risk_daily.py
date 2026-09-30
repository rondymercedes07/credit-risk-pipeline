"""Daily credit-risk pipeline: download -> load RAW -> dbt (Cosmos task group) -> verify.

The dbt project is rendered by Astronomer Cosmos as one Airflow task per model (plus one per
model's tests), so a failure points at the exact model and a rerun can resume from it. dbt lives
in a separate virtualenv (/opt/venv) because its dependency pins conflict with Airflow's; Cosmos
only needs the path to its executable. The SCD2 snapshot and the tests that read it are excluded
here: they belong to the `replay_loan_status` DAG (simulated monthly loads), triggered by hand.

The `target` param selects the warehouse: `ci` (default) runs on DuckDB with the committed
sample and needs no credentials; `dev` / `prod` run on Snowflake with the full dataset.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task
from airflow.task.trigger_rule import TriggerRule
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

PROJECT = "/opt/project"
DBT_PROJECT = f"{PROJECT}/dbt"
DBT_EXECUTABLE = "/opt/venv/bin/dbt"
TARGET = "{{ params.target }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
    # One slot on ci: DuckDB allows a single writing process. Raise DBT_POOL_SLOTS for Snowflake.
    "pool": "dbt_warehouse",
}

# target_name is only used at parse time (dbt ls); the runtime target comes from the DAG param
# through dbt_cmd_flags below, which is templated.
profile_config = ProfileConfig(
    profile_name="credit_risk",
    target_name="ci",
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",
)

with DAG(
    dag_id="credit_risk_daily",
    description="Home Credit pipeline: Kaggle -> RAW -> dbt (staging, marts, tests) -> checks",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,  # runs share one warehouse (and one DuckDB file on ci)
    default_args=default_args,
    params={
        "target": Param("ci", enum=["ci", "dev", "prod"], description="dbt / loader target"),
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
    dbt_deps = BashOperator(task_id="dbt_deps", bash_command="dbt deps", cwd=DBT_PROJECT)

    # Tests: severity error fails the task (and the DAG); severity warn exits 0 and is only logged.
    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        # Packages come from the dbt_deps task, not from every Cosmos task.
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT, install_dbt_deps=False),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            exclude=["snp_loan_status+"],
        ),
        operator_args={
            "dbt_cmd_flags": ["--target", TARGET],
        },
    )

    verify = BashOperator(
        task_id="verify",
        bash_command=f"python scripts/verify_pipeline.py --target {TARGET}",
        cwd=PROJECT,
    )

    branch >> [skip_download, download_data] >> load_raw
    load_raw >> dbt_deps >> dbt_build >> verify
