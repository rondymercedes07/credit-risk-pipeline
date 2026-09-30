# Airflow image with the project's Python environment (dbt + loaders) in a separate venv.
# Build context is the repository root (see docker-compose.yml).
FROM apache/airflow:3.1.8-python3.12

# dbt-core / dbt-snowflake pin dependencies that clash with Airflow's constraints, so they get
# their own virtualenv; DAG tasks call its executables (PATH below) via BashOperator.
USER root
RUN python -m venv /opt/venv && chown -R airflow /opt/venv
USER airflow
# Cosmos renders the dbt project as Airflow tasks; it is installed in Airflow's own environment
# (under Airflow's constraints, so it cannot break it) while dbt itself stays in /opt/venv.
COPY orchestration/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
RUN mkdir /tmp/project
COPY --chown=airflow pyproject.toml README.md /tmp/project/
COPY --chown=airflow credit_risk_pipeline /tmp/project/credit_risk_pipeline
RUN /opt/venv/bin/pip install --no-cache-dir "/tmp/project[dbt]"

# The repository itself is bind-mounted at /opt/project at run time (live code, shared data/).
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH="/opt/project" \
    DBT_PROFILES_DIR="/opt/project/dbt" \
    DBT_SEND_ANONYMOUS_USAGE_STATS="false"
