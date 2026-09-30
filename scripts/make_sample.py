"""Build a deterministic, referentially complete sample of the Home Credit data for CI.

Selects N clients from application_train by ordering md5(seed || ':' || SK_ID_CURR)
(stable across DuckDB versions and platforms, unlike RANDOM()/hash()), then keeps every
related record: bureau, bureau_balance (via SK_ID_BUREAU), previous_application,
installments_payments, POS_CASH_balance and credit_card_balance.

The sample is NOT stratified on TARGET, so it preserves the population default rate.
Values are kept as text (same as the RAW layer). Output is zstd parquet, sorted for
reproducible content, plus manifest.json. Fails if the total size exceeds --max-mb.

Usage:
    python scripts/make_sample.py [--n-clients 5000] [--seed 42] [--max-mb 20]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

from credit_risk_pipeline.config import RAW_DIR, SAMPLE_DIR
from credit_risk_pipeline.tables import RAW_TABLES, TABLES_BY_NAME


def _csv(name: str, raw_dir: Path) -> str:
    path = (raw_dir / TABLES_BY_NAME[name].csv_file).as_posix()
    return f"read_csv('{path}', all_varchar=true, header=true)"


def build_sample(raw_dir: Path, out_dir: Path, n_clients: int, seed: int) -> dict:
    missing = [t.csv_file for t in RAW_TABLES if not (raw_dir / t.csv_file).exists()]
    if missing:
        sys.exit(f"Missing files in {raw_dir}: {missing}. Run scripts/download_data.py first.")
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(
        f"""
        CREATE TEMP TABLE sample_clients AS
        SELECT SK_ID_CURR
        FROM {_csv("application_train", raw_dir)}
        ORDER BY md5('{seed}:' || SK_ID_CURR), SK_ID_CURR
        LIMIT {int(n_clients)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE sample_bureau AS
        SELECT SK_ID_BUREAU FROM {_csv("bureau", raw_dir)}
        WHERE SK_ID_CURR IN (SELECT SK_ID_CURR FROM sample_clients)
        """
    )

    counts: dict[str, int] = {}
    for table in RAW_TABLES:
        if table.name == "bureau_balance":
            key, keys_table = "SK_ID_BUREAU", "sample_bureau"
        else:
            key, keys_table = "SK_ID_CURR", "sample_clients"
        target = (out_dir / table.sample_file).as_posix()
        con.execute(
            f"""
            COPY (
                SELECT * FROM {_csv(table.name, raw_dir)}
                WHERE {key} IN (SELECT {key} FROM {keys_table})
                ORDER BY ALL
            ) TO '{target}' (FORMAT parquet, COMPRESSION zstd)
            """
        )
        counts[table.name] = con.execute(
            f"SELECT count(*) FROM read_parquet('{target}')"
        ).fetchone()[0]

    ids = [r[0] for r in con.execute("SELECT SK_ID_CURR FROM sample_clients ORDER BY 1").fetchall()]
    app_sample = (out_dir / TABLES_BY_NAME["application_train"].sample_file).as_posix()
    default_rate = con.execute(
        f"""
        SELECT avg(CASE WHEN TARGET = '1' THEN 1.0 ELSE 0.0 END)
        FROM read_parquet('{app_sample}')
        """
    ).fetchone()[0]
    manifest = {
        "seed": seed,
        "n_clients": len(ids),
        "selection": "ORDER BY md5(seed || ':' || SK_ID_CURR) LIMIT n_clients",
        "client_ids_sha256": hashlib.sha256(",".join(ids).encode()).hexdigest(),
        "default_rate": round(default_rate, 6),
        "row_counts": counts,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=SAMPLE_DIR)
    parser.add_argument("--n-clients", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-mb", type=float, default=20.0)
    args = parser.parse_args()

    manifest = build_sample(args.raw_dir, args.out_dir, args.n_clients, args.seed)
    total_mb = sum(p.stat().st_size for p in args.out_dir.glob("*.parquet")) / 1e6
    print(json.dumps(manifest, indent=2))
    print(f"Total parquet size: {total_mb:.1f} MB (limit {args.max_mb} MB)")
    if total_mb > args.max_mb:
        sys.exit(
            "Sample exceeds the size limit: do NOT commit it. Lower --n-clients or "
            "host it as a release asset / download it in CI."
        )


if __name__ == "__main__":
    main()
