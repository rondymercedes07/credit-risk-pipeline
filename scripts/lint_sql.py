"""Run sqlfluff on the dbt SQL with the ci DuckDB file resolved from the repo root.

The sqlfluff dbt templater compiles the project, which needs the ci database
(`python scripts/load_raw.py --target ci`). dbt/profiles.yml defaults to a path relative to the
dbt folder, which is wrong when sqlfluff runs from the repo root, so this wrapper sets
DUCKDB_PATH to an absolute path unless you already did.

Usage:
    python scripts/lint_sql.py                      # whole dbt project
    python scripts/lint_sql.py dbt/models/marts     # or specific paths (pre-commit passes files)
"""

from __future__ import annotations

import os
import subprocess
import sys

from credit_risk_pipeline import config

DEFAULT_PATHS = ["dbt/models", "dbt/tests", "dbt/snapshots"]


def main() -> int:
    os.environ.setdefault("DUCKDB_PATH", str(config.DEFAULT_DUCKDB_PATH))
    paths = sys.argv[1:] or DEFAULT_PATHS
    return subprocess.call([sys.executable, "-m", "sqlfluff", "lint", *paths], cwd=config.REPO_ROOT)


if __name__ == "__main__":
    sys.exit(main())
