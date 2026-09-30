"""Download the Home Credit Default Risk source files from Kaggle into data/raw/.

Only the eight tables used by this project are fetched (the seven Home Credit tables plus
application_test, which explains the orphan keys in the related tables; not the submission
template, etc.), one file at a time so a partial run can be resumed.

Credentials are never read by this script directly: the official Kaggle client resolves
them (~/.kaggle/kaggle.json, KAGGLE_USERNAME/KAGGLE_KEY, KAGGLE_API_TOKEN, or OAuth via
`kaggle auth login`). You must also accept the competition rules on kaggle.com, otherwise
the API answers 403 even with valid credentials.

Usage:
    python scripts/download_data.py [--dest data/raw] [--force]
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

from credit_risk_pipeline.config import RAW_DIR
from credit_risk_pipeline.tables import COMPETITION, RAW_TABLES, RawTable

RULES_URL = f"https://www.kaggle.com/competitions/{COMPETITION}/rules"


def check_header(path: Path, table: RawTable) -> None:
    """Fail fast if a downloaded file does not look like the expected table."""
    with path.open(encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
    missing = [c for c in table.required_columns if c not in header]
    if missing:
        raise ValueError(f"{path.name}: missing expected columns {missing}; got {header[:8]}...")


def _unzip_if_needed(dest: Path, file_name: str) -> Path:
    target = dest / file_name
    archive = dest / f"{file_name}.zip"
    if archive.exists():
        with zipfile.ZipFile(archive) as zf:
            zf.extract(file_name, dest)
        archive.unlink()
    if not target.exists():
        raise FileNotFoundError(f"Kaggle did not produce {target}")
    return target


def _authenticate():
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()  # the client calls exit(1) when no credentials are found
    except SystemExit:
        sys.exit(
            "Kaggle credentials not found. Put your token in ~/.kaggle/kaggle.json "
            "(kaggle.com > Settings > API) or run `kaggle auth login`."
        )
    return api


def download(dest: Path, force: bool) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    pending = [t for t in RAW_TABLES if force or not (dest / t.csv_file).exists()]
    if not pending:
        print(f"All {len(RAW_TABLES)} files already present in {dest}. Use --force to re-download.")
    else:
        api = _authenticate()
        for table in pending:
            print(f"Downloading {table.csv_file} ...")
            try:
                api.competition_download_file(
                    COMPETITION, table.csv_file, path=str(dest), force=True
                )
            except Exception as exc:  # the client raises library-specific HTTP errors
                if "403" in str(exc) or "Forbidden" in str(exc):
                    sys.exit(
                        f"403 Forbidden for {COMPETITION}. Accept the rules first: {RULES_URL}"
                    )
                raise
            _unzip_if_needed(dest, table.csv_file)

    for table in RAW_TABLES:
        path = dest / table.csv_file
        check_header(path, table)
        print(f"OK  {table.csv_file:<28} {path.stat().st_size / 1e6:>9.1f} MB")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dest", type=Path, default=RAW_DIR, help="output directory")
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args()
    download(args.dest, args.force)


if __name__ == "__main__":
    main()
