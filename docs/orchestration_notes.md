# Orchestration notes

## Cosmos patch: the DAG `target` param must be the effective dbt target

**File:** `orchestration/airflow/dags/credit_risk_daily.py` (`_use_templated_target`).
**Pinned version:** `astronomer-cosmos==1.15.1` (`orchestration/airflow/requirements.txt`).

### Symptom
Triggering `credit_risk_daily` with `target=dev` ran the dbt tasks against the `ci` DuckDB file. The
task log showed the effective command ended with `--target dev ... --target ci`, dbt reported
`Concurrency: 4 threads (target='ci')`, and tasks failed with `IO Error: Could not set lock on file`
(several tasks writing the same DuckDB file). Nothing reached Snowflake, but the run was meaningless.

### Cause
`ProfileConfig.target_name` is a plain string, not a template. Cosmos always appends
`--target <ProfileConfig.target_name>` AFTER the user's `dbt_cmd_flags`, and dbt keeps the last value of a
repeated flag. So the templated `--target {{ params.target }}` we pass in `operator_args["dbt_cmd_flags"]`
was silently overridden by the static `ci` used at parse time.

### Fix
`AbstractDbtLocalBase._generate_dbt_flags` is wrapped: when the operator carries our `--target` in
`dbt_cmd_flags`, Cosmos's static `--target <name>` pair is removed, so the single remaining `--target` is the
rendered DAG param. The wrapper is installed once per process (guarded by a flag) when the DAG file is imported,
which also happens in the task process.

Alternatives considered: one `DbtTaskGroup` per target behind skip/short-circuit tasks (no monkeypatch, but
three copies of the 37-task graph and three `dbt ls` at every parse, including `dev`/`prod`, which need
credentials the dag-processor should not require); and reading the target from an environment variable at
parse time (loses the per-run param).

### How a Cosmos change is detected
- The wrapper raises `RuntimeError("Unexpected Cosmos flags ...")` if the generated flags do not contain
  exactly one `--target`, so an upgrade that changes the flag handling fails the task loudly instead of
  running on the wrong warehouse.
- `tests/test_dags.py::test_dbt_tasks_run_against_the_dag_param_target_not_cosmos_static_one` (parametrized over
  `ci`/`dev`/`prod`) builds the real flags of a Cosmos task and asserts there is exactly one `--target` and it is
  the requested one. It runs in the `dags` CI job.

### Review on every Cosmos upgrade
1. Bump the pin and rebuild the image.
2. Run `tests/test_dags.py` in the image. If the test fails, the private method changed.
3. Check whether Cosmos now supports a templated or per-run target (changelog of `ProfileConfig` and
   `operator_args`). If it does, delete the patch and keep the test.
4. Do one `dev` run and confirm `Concurrency: N threads (target='dev')` in a `dbt_build.*` task log.

## Other orchestration decisions

- **dbt in its own virtualenv** (`/opt/venv`): dbt's dependency pins conflict with Airflow's. Cosmos only needs
  the executable path. Consequence: tests that import `duckdb`/dbt (`test_pipeline_scripts.py`) run in the project
  venv or CI, not inside the Airflow image; `test_dags.py` is the reverse.
- **Pool `dbt_warehouse`** serializes warehouse-writing tasks. Default 1 slot, safe for DuckDB; raise it live
  for Snowflake (see README). A single pool serves both targets because Airflow's `pool` is not templated.
- **`needs_download`** checks `data/raw` and downloads only when a file is missing; `ci` never downloads.
- **`load_raw` skips unchanged tables** (sha256 + row count in `RAW._LOAD_AUDIT`), which is what makes a daily
  schedule on the full dataset cheap: an unchanged day costs the hashing of the files, not a 58M-row reload.
- **dag-processor runs `dbt deps` before starting**: Cosmos renders the graph with `dbt ls` at parse time, so the
  packages must exist first. Its health endpoint reports unhealthy until that finishes (about a minute).
