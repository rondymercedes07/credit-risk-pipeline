"""Tests for the Airflow DAGs.

The DagBag tests need Airflow and Cosmos, which live in the Airflow image, not in the project
venv: run them there with

    docker compose -f orchestration/airflow/docker-compose.yml run --rm --no-deps \\
        --entrypoint bash airflow-scheduler -c "cd /opt/project && pytest tests/test_dags.py"

Elsewhere they are skipped. The secrets scan needs nothing and always runs.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
AIRFLOW_DIR = REPO / "orchestration" / "airflow"
DAGS_DIR = AIRFLOW_DIR / "dags"


@pytest.fixture(scope="module")
def dagbag():
    pytest.importorskip("airflow")
    pytest.importorskip("cosmos")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_both_dags_exist(dagbag):
    assert {"credit_risk_daily", "replay_loan_status"} <= set(dagbag.dags)


def test_daily_dag_is_paused_on_creation_and_does_not_catch_up(dagbag):
    dag = dagbag.dags["credit_risk_daily"]
    assert dag.is_paused_upon_creation is True
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == "@daily"


def test_daily_dag_target_param_defaults_to_ci(dagbag):
    params = dagbag.dags["credit_risk_daily"].params
    assert params["target"] == "ci"


def test_replay_dag_is_manual_only_with_expected_params(dagbag):
    dag = dagbag.dags["replay_loan_status"]
    assert dag.schedule is None
    assert dag.params["start_month"] == -12
    assert dag.params["end_month"] == -1
    assert dag.params["target"] == "ci"
    assert "monthly loads" in dag.description


def test_daily_dag_dependencies(dagbag):
    dag = dagbag.dags["credit_risk_daily"]
    task = dag.get_task
    assert task("load_raw").upstream_task_ids == {"skip_download", "download_data"}
    assert task("dbt_deps").upstream_task_ids == {"load_raw"}
    assert task("verify").upstream_task_ids, "verify must wait for the dbt task group"

    dbt_tasks = [t for t in dag.tasks if t.task_id.startswith("dbt_build.")]
    assert dbt_tasks, "the Cosmos task group rendered no tasks"
    roots = [t for t in dbt_tasks if not (t.upstream_task_ids & {x.task_id for x in dbt_tasks})]
    assert all("dbt_deps" in t.upstream_task_ids for t in roots)
    assert all(
        t.task_id.startswith("dbt_build.") for t in map(task, task("verify").upstream_task_ids)
    )


def test_daily_dag_leaves_snapshot_to_the_replay_dag(dagbag):
    ids = {t.task_id for t in dagbag.dags["credit_risk_daily"].tasks}
    assert not any("snp_loan_status" in i or "snapshot" in i for i in ids)


def test_replay_dag_dependencies(dagbag):
    task = dagbag.dags["replay_loan_status"].get_task
    assert task("replay_snapshot").upstream_task_ids == set()
    assert task("test_snapshot").upstream_task_ids == {"replay_snapshot"}
    assert task("verify_snapshot").upstream_task_ids == {"test_snapshot"}


@pytest.mark.parametrize("dag_id", ["credit_risk_daily", "replay_loan_status"])
def test_every_task_retries(dagbag, dag_id):
    for t in dagbag.dags[dag_id].tasks:
        assert t.retries >= 1, f"{dag_id}.{t.task_id} has no retries"


# Files of the orchestration layer that must never carry a credential.
SCANNED = [*DAGS_DIR.glob("*.py"), AIRFLOW_DIR / "Dockerfile", AIRFLOW_DIR / "docker-compose.yml"]
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "Kaggle token": re.compile(r"KGAT_[A-Za-z0-9]+"),
    "password with a value": re.compile(r"(?i)(password|passphrase)\s*[:=]\s*['\"]?[^\s'\"$]{3,}"),
    "Fernet key": re.compile(r"\b[A-Za-z0-9_-]{43}=(?![A-Za-z0-9])"),
    "JWT / secret with a value": re.compile(
        r"(?i)(jwt_secret|secret_key)\s*[:=]\s*['\"]?[^\s'\"$]+"
    ),
}


# The metadata Postgres is internal to the compose network (no published port) and holds no
# business data; its password is the one literal allowed, spelled out here so it stays visible.
LOCAL_DB_PASSWORD = "POSTGRES_PASSWORD: airflow"


@pytest.mark.parametrize("path", SCANNED, ids=lambda p: p.name)
def test_no_credentials_in_orchestration_code(path):
    text = path.read_text(encoding="utf-8")
    text = text.replace(LOCAL_DB_PASSWORD, "")  # see below
    hits = [name for name, rx in SECRET_PATTERNS.items() if rx.search(text)]
    assert not hits, f"{path.name} looks like it contains: {hits}"
