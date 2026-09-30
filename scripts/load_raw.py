"""Load the source tables into the RAW layer of a target warehouse.

RAW contract: every source column is stored as text (VARCHAR), exactly as delivered, plus
two audit columns (_loaded_at, _source_file). Typing and cleansing happen in dbt staging.
Each table is reloaded atomically, so re-running never duplicates rows.

Unchanged tables are skipped: RAW._LOAD_AUDIT (raw._load_audit on DuckDB) records, per table,
the sha256 of the source file and the row count of the last successful load. A table is
skipped only when the file hash matches AND the table in RAW still has the recorded row
count; --force reloads everything regardless.

Targets:
    ci    DuckDB file (data/ci/credit_risk.duckdb), default source: data/sample/*.parquet
    dev   Snowflake CREDIT_RISK_DEV,  default source: data/raw/*.csv
    prod  Snowflake CREDIT_RISK_PROD, default source: data/raw/*.csv

Usage:
    python scripts/load_raw.py --target ci
    python scripts/load_raw.py --target dev [--source sample] [--tables bureau,bureau_balance]
    python scripts/load_raw.py --target dev --force      # reload even if nothing changed
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
import tempfile
from pathlib import Path

import duckdb

from credit_risk_pipeline import config
from credit_risk_pipeline.tables import AUDIT_COLUMNS, RAW_TABLES, TABLES_BY_NAME, RawTable
from credit_risk_pipeline.warehouse import check_identifier, sql_string

# --------------------------------------------------------------------------- sources


def source_path(table: RawTable, source: str, raw_dir: Path, sample_dir: Path) -> Path:
    path = raw_dir / table.csv_file if source == "full" else sample_dir / table.sample_file
    if not path.exists():
        hint = "scripts/download_data.py" if source == "full" else "scripts/make_sample.py"
        raise FileNotFoundError(f"{path} not found. Run {hint} first.")
    return path


def read_relation(path: Path) -> str:
    """DuckDB table function reading a source file with every column as text."""
    p = path.as_posix()
    if path.suffix == ".parquet":
        return f"read_parquet('{p}')"
    return f"read_csv('{p}', all_varchar=true, header=true)"


def source_columns(path: Path) -> list[str]:
    if path.suffix == ".parquet":
        rows = duckdb.connect().execute(f"DESCRIBE SELECT * FROM {read_relation(path)}").fetchall()
        cols = [r[0] for r in rows]
    else:
        with path.open(encoding="utf-8", newline="") as fh:
            cols = next(csv.reader(fh))
    return [check_identifier(c) for c in cols]


def count_source_rows(path: Path) -> int:
    return duckdb.connect().execute(f"SELECT count(*) FROM {read_relation(path)}").fetchone()[0]


# --------------------------------------------------------------------------- load audit / skip

AUDIT_TABLE = "_load_audit"
AUDIT_COLUMNS_DDL = (
    "table_name VARCHAR, source_file VARCHAR, sha256 VARCHAR, row_count BIGINT, loaded_at TIMESTAMP"
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def skip_decision(
    source_sha: str,
    audit: tuple[str, int] | None,
    raw_rows: int | None,
    force: bool,
) -> tuple[bool, str]:
    """Decide whether a table can be skipped. Returns (skip, reason).

    audit is (sha256, row_count) of the last successful load, raw_rows the current row count of
    the RAW table (None if it does not exist). Skipping requires all of: not forced, a previous
    load on record, the same source hash, and a non-empty RAW table that still has the recorded
    row count (so a truncated or tampered table is reloaded even if the file did not change).
    """
    if force:
        return False, "forced"
    if audit is None:
        return False, "no previous load on record"
    audited_sha, audited_rows = audit
    if audited_sha != source_sha:
        return False, "source file changed"
    if not raw_rows:
        return False, "RAW table is missing or empty"
    if raw_rows != audited_rows:
        return False, f"RAW has {raw_rows} rows, audit recorded {audited_rows}"
    return True, "unchanged"


def report(name: str, skipped: bool, reason: str) -> None:
    status = "SKIP" if skipped else "LOAD"
    print(f"{status} raw.{name:<24} {'skipped: ' if skipped else ''}{reason}")


# --------------------------------------------------------------------------- DuckDB (ci)


def load_duckdb(
    tables: list[RawTable],
    source: str,
    db_path: Path,
    raw_dir: Path,
    sample_dir: Path,
    force: bool = False,
) -> dict[str, str]:
    """Load the tables; returns {table: "loaded" | "skipped"}."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {config.RAW_SCHEMA}")
    audit_fq = f"{config.RAW_SCHEMA}.{AUDIT_TABLE}"
    con.execute(f"CREATE TABLE IF NOT EXISTS {audit_fq} ({AUDIT_COLUMNS_DDL})")
    outcome: dict[str, str] = {}
    for table in tables:
        path = source_path(table, source, raw_dir, sample_dir)
        sha = file_sha256(path)
        row = con.execute(
            f"SELECT sha256, row_count FROM {audit_fq} WHERE table_name = ?", [table.name]
        ).fetchone()
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? "
            "AND table_name = ?",
            [config.RAW_SCHEMA, table.name],
        ).fetchone()[0]
        raw_rows = (
            con.execute(f"SELECT count(*) FROM {config.RAW_SCHEMA}.{table.name}").fetchone()[0]
            if exists
            else None
        )
        skip, reason = skip_decision(sha, row, raw_rows, force)
        report(table.name, skip, reason)
        outcome[table.name] = "skipped" if skip else "loaded"
        if skip:
            continue
        expected = count_source_rows(path)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {config.RAW_SCHEMA}.{table.name} AS
            SELECT s.*,
                   current_timestamp::TIMESTAMP AS _loaded_at,
                   {sql_string(path.name)} AS _source_file
            FROM {read_relation(path)} AS s
            """
        )
        loaded = con.execute(f"SELECT count(*) FROM {config.RAW_SCHEMA}.{table.name}").fetchone()[0]
        _reconcile(table.name, expected, loaded)
        con.execute(f"DELETE FROM {audit_fq} WHERE table_name = ?", [table.name])
        con.execute(
            f"INSERT INTO {audit_fq} VALUES (?, ?, ?, ?, current_timestamp::TIMESTAMP)",
            [table.name, path.name, sha, loaded],
        )
    con.close()
    return outcome


def _reconcile(name: str, expected: int, loaded: int) -> None:
    if expected != loaded:
        raise RuntimeError(f"{name}: row count mismatch (source={expected}, loaded={loaded})")
    print(f"OK  raw.{name:<24} {loaded:>12,} rows")


# --------------------------------------------------------------------------- Snowflake SQL


def sf_create_table_sql(fq_table: str, columns: list[str]) -> str:
    cols = ",\n    ".join(f"{check_identifier(c)} VARCHAR" for c in columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {fq_table} (\n    {cols},\n"
        "    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),\n"
        "    _source_file VARCHAR\n)"
    )


def sf_copy_sql(fq_table: str, columns: list[str], stage_file: str, source_name: str) -> str:
    targets = ", ".join(columns + ["_source_file"])
    selects = ", ".join([f"${i}" for i in range(1, len(columns) + 1)] + [sql_string(source_name)])
    return (
        f"COPY INTO {fq_table} ({targets})\n"
        f"FROM (SELECT {selects} FROM @raw_load_stage/{stage_file})\n"
        "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"'\n"
        "               EMPTY_FIELD_AS_NULL = TRUE NULL_IF = ('') COMPRESSION = GZIP)\n"
        "ON_ERROR = ABORT_STATEMENT FORCE = TRUE"
    )


def sf_expected_columns(columns: list[str]) -> list[str]:
    return [c.upper() for c in columns] + [c.upper() for c in AUDIT_COLUMNS]


# --------------------------------------------------------------------------- Snowflake load


def load_snowflake(
    tables: list[RawTable],
    source: str,
    target: str,
    raw_dir: Path,
    sample_dir: Path,
    force: bool = False,
) -> dict[str, str]:
    """Load the tables; returns {table: "loaded" | "skipped"}."""
    missing = config.missing_snowflake_env()
    if missing:
        sys.exit(
            f"Missing Snowflake environment variables: {', '.join(missing)} (see .env.example)"
        )
    import snowflake.connector  # imported lazily: not needed for the ci target

    con = snowflake.connector.connect(**config.snowflake_connect_kwargs(target))
    outcome: dict[str, str] = {}
    try:
        cur = con.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS RAW.{AUDIT_TABLE.upper()} ({AUDIT_COLUMNS_DDL})")
        cur.execute("CREATE TEMPORARY STAGE raw_load_stage")
        for table in tables:
            path = source_path(table, source, raw_dir, sample_dir)
            sha = file_sha256(path)
            skip, reason = skip_decision(
                sha, _sf_audit(cur, table), _sf_raw_rows(cur, table), force
            )
            report(table.name, skip, reason)
            outcome[table.name] = "skipped" if skip else "loaded"
            if skip:
                continue
            with tempfile.TemporaryDirectory() as tmp:
                upload = path
                if path.suffix == ".parquet":  # sample -> csv.gz so one COPY path serves both
                    upload = Path(tmp) / f"{table.name}.csv.gz"
                    duckdb.connect().execute(
                        f"COPY (SELECT * FROM {read_relation(path)}) TO '{upload.as_posix()}' "
                        "(FORMAT csv, HEADER, COMPRESSION gzip)"
                    )
                loaded = _load_snowflake_table(cur, table, path, upload)
            _sf_record(cur, table, path, sha, loaded)
    finally:
        con.close()
    return outcome


def _sf_audit(cur, table: RawTable) -> tuple[str, int] | None:
    row = cur.execute(
        f"SELECT sha256, row_count FROM RAW.{AUDIT_TABLE.upper()} WHERE table_name = %s",
        (table.name,),
    ).fetchone()
    return (row[0], int(row[1])) if row else None


def _sf_raw_rows(cur, table: RawTable) -> int | None:
    exists = cur.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'RAW' AND table_name = %s",
        (table.name.upper(),),
    ).fetchone()[0]
    if not exists:
        return None
    return int(cur.execute(f"SELECT count(*) FROM RAW.{table.name.upper()}").fetchone()[0])


def _sf_record(cur, table: RawTable, path: Path, sha: str, rows: int) -> None:
    fq = f"RAW.{AUDIT_TABLE.upper()}"
    cur.execute(f"DELETE FROM {fq} WHERE table_name = %s", (table.name,))
    cur.execute(
        f"INSERT INTO {fq} VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())",
        (table.name, path.name, sha, rows),
    )


def _load_snowflake_table(cur, table: RawTable, source: Path, upload: Path) -> int:
    columns = source_columns(source)
    expected_rows = count_source_rows(source)
    fq, fq_new = f"RAW.{table.name.upper()}", f"RAW.{table.name.upper()}__NEW"

    cur.execute(sf_create_table_sql(fq, columns))
    existing = [
        r[0]
        for r in cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'RAW' AND table_name = %s ORDER BY ordinal_position",
            (table.name.upper(),),
        ).fetchall()
    ]
    if existing != sf_expected_columns(columns):
        raise RuntimeError(f"{fq}: schema drift. Table has {existing}, source has {columns}")

    stage_file = upload.name if upload.name.endswith(".gz") else f"{upload.name}.gz"
    cur.execute(
        f"PUT file://{upload.as_posix()} @raw_load_stage AUTO_COMPRESS = TRUE OVERWRITE = TRUE"
    )
    cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {fq_new} LIKE {fq}")
    cur.execute(sf_copy_sql(fq_new, columns, stage_file, source.name))
    loaded = cur.execute(f"SELECT count(*) FROM {fq_new}").fetchone()[0]
    _reconcile(fq_new, expected_rows, loaded)
    cur.execute(f"ALTER TABLE {fq} SWAP WITH {fq_new}")  # atomic publish
    cur.execute(f"DROP TABLE {fq_new}")
    return int(loaded)


# --------------------------------------------------------------------------- CLI


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target", choices=config.TARGETS, required=True)
    parser.add_argument(
        "--source", choices=("full", "sample"), help="default: sample for ci, full otherwise"
    )
    parser.add_argument("--tables", help="comma-separated subset of table names")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR)
    parser.add_argument("--sample-dir", type=Path, default=config.SAMPLE_DIR)
    parser.add_argument("--db-path", type=Path, help="DuckDB file (ci only)")
    parser.add_argument(
        "--force", action="store_true", help="reload every table even if its source is unchanged"
    )
    args = parser.parse_args()

    source = args.source or ("sample" if args.target == "ci" else "full")
    names = args.tables.split(",") if args.tables else [t.name for t in RAW_TABLES]
    unknown = [n for n in names if n not in TABLES_BY_NAME]
    if unknown:
        sys.exit(f"Unknown tables: {unknown}. Valid: {list(TABLES_BY_NAME)}")
    tables = [TABLES_BY_NAME[n] for n in names]

    if args.target == "ci":
        load_duckdb(
            tables,
            source,
            args.db_path or config.duckdb_path(),
            args.raw_dir,
            args.sample_dir,
            args.force,
        )
    else:
        load_snowflake(tables, source, args.target, args.raw_dir, args.sample_dir, args.force)


if __name__ == "__main__":
    main()
