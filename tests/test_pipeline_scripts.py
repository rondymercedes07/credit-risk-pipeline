"""Tests for the sampling and loading scripts, run on SYNTHETIC fixture data only."""

from __future__ import annotations

import duckdb
import load_raw
import make_sample
import pytest

from credit_risk_pipeline.tables import RAW_TABLES
from credit_risk_pipeline.warehouse import check_identifier, sql_string


def test_sample_is_deterministic_and_complete(synthetic_raw, tmp_path):
    a = make_sample.build_sample(synthetic_raw, tmp_path / "a", n_clients=10, seed=42)
    b = make_sample.build_sample(synthetic_raw, tmp_path / "b", n_clients=10, seed=42)
    c = make_sample.build_sample(synthetic_raw, tmp_path / "c", n_clients=10, seed=7)
    assert a == b
    assert a["client_ids_sha256"] != c["client_ids_sha256"]
    assert a["n_clients"] == 10
    # every client keeps all of its 2 bureau records and every bureau record its 3 months
    assert a["row_counts"]["bureau"] == 20
    assert a["row_counts"]["bureau_balance"] == 60


def test_sample_has_no_orphans(synthetic_raw, tmp_path):
    make_sample.build_sample(synthetic_raw, tmp_path, n_clients=10, seed=1)
    con = duckdb.connect()
    orphans = con.execute(
        f"""SELECT count(*) FROM read_parquet('{tmp_path / "bureau_balance.parquet"}') bb
            WHERE SK_ID_BUREAU NOT IN
              (SELECT SK_ID_BUREAU FROM read_parquet('{tmp_path / "bureau.parquet"}'))"""
    ).fetchone()[0]
    assert orphans == 0


def test_load_duckdb_full_and_idempotent(synthetic_raw, tmp_path):
    db = tmp_path / "ci.duckdb"
    for _ in range(2):  # re-running must not duplicate rows
        load_raw.load_duckdb(list(RAW_TABLES), "full", db, synthetic_raw, tmp_path)
    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM raw.application_train").fetchone()[0] == 40
    cols = [r[0] for r in con.execute("DESCRIBE raw.application_train").fetchall()]
    assert cols[-2:] == ["_loaded_at", "_source_file"]
    types = {r[0]: r[1] for r in con.execute("DESCRIBE raw.application_train").fetchall()}
    assert types["TARGET"] == "VARCHAR"
    # empty CSV fields land as NULL, not empty strings
    assert (
        con.execute(
            "SELECT count(*) FROM raw.application_train WHERE AMT_INCOME_TOTAL IS NULL"
        ).fetchone()[0]
        == 40
    )


def test_load_duckdb_from_sample(synthetic_raw, tmp_path):
    make_sample.build_sample(synthetic_raw, tmp_path / "s", n_clients=5, seed=1)
    db = tmp_path / "ci.duckdb"
    load_raw.load_duckdb(list(RAW_TABLES), "sample", db, synthetic_raw, tmp_path / "s")
    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM raw.application_train").fetchone()[0] == 5


def test_missing_source_gives_actionable_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="download_data.py"):
        load_raw.source_path(RAW_TABLES[0], "full", tmp_path, tmp_path)


def test_snowflake_sql_builders():
    ddl = load_raw.sf_create_table_sql("RAW.BUREAU", ["SK_ID_CURR", "SK_ID_BUREAU"])
    assert "SK_ID_CURR VARCHAR" in ddl and "_loaded_at TIMESTAMP_NTZ" in ddl
    copy = load_raw.sf_copy_sql("RAW.BUREAU__NEW", ["A", "B"], "bureau.csv.gz", "bureau.csv")
    assert "(A, B, _source_file)" in copy and "$1, $2, 'bureau.csv'" in copy
    assert "ON_ERROR = ABORT_STATEMENT" in copy


def test_identifier_and_literal_safety():
    assert check_identifier("SK_ID_CURR") == "SK_ID_CURR"
    with pytest.raises(ValueError):
        check_identifier("x; DROP TABLE y")
    assert sql_string("o'brien") == "'o''brien'"
