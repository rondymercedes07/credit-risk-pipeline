"""Paths and connection settings. Secrets come from the environment only."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
SAMPLE_DIR = REPO_ROOT / "data" / "sample"
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "ci" / "credit_risk.duckdb"

RAW_SCHEMA = "raw"
TARGETS = ("ci", "dev", "prod")

load_dotenv(REPO_ROOT / ".env")


def duckdb_path() -> Path:
    return Path(os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH)


def snowflake_database(target: str) -> str:
    if target == "dev":
        return os.environ.get("SNOWFLAKE_DATABASE_DEV") or "CREDIT_RISK_DEV"
    if target == "prod":
        return os.environ.get("SNOWFLAKE_DATABASE_PROD") or "CREDIT_RISK_PROD"
    raise ValueError(f"target {target!r} is not a Snowflake target")


def missing_snowflake_env() -> list[str]:
    """Names of required Snowflake variables that are not set."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"]
    missing = [name for name in required if not os.environ.get(name)]
    if not (os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH") or os.environ.get("SNOWFLAKE_PASSWORD")):
        missing.append("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
    return missing


def snowflake_connect_kwargs(target: str) -> dict[str, str]:
    """Keyword arguments for snowflake.connector.connect (key-pair preferred)."""
    kwargs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE") or "CREDIT_RISK_ENGINEER",
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE") or "CREDIT_RISK_WH",
        "database": snowflake_database(target),
        "schema": RAW_SCHEMA.upper(),
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        kwargs["private_key_file"] = key_path
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if passphrase:
            kwargs["private_key_file_pwd"] = passphrase
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return kwargs
