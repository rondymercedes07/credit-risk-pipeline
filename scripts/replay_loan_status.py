"""Replay the loan status history as SIMULATED monthly loads into the SCD2 snapshot.

The source data is static, so a dbt snapshot run against it never sees a change. This script
reproduces the history the data does contain: for each month m from --start to --end it runs
`dbt snapshot --vars '{as_of_month: m}'`, so the snapshot sees, at load m, the latest status of
every POS / cash loan with months_balance <= m. dbt_valid_from / dbt_valid_to of the result are the
real wall-clock times of these runs, not business dates; the business timeline is as_of_month.

Usage:
    python scripts/replay_loan_status.py --target ci --start -96 --end -1 --fresh
    python scripts/replay_loan_status.py --target dev --start -12 --end -1 --fresh
"""

from __future__ import annotations

import argparse
import subprocess
import sys

from credit_risk_pipeline import config

DBT = config.REPO_ROOT / ".venv" / "Scripts" / "dbt.exe"
DBT_DIR = config.REPO_ROOT / "dbt"


def run_dbt(*args: str) -> None:
    cmd = [str(DBT) if DBT.exists() else "dbt", *args]
    result = subprocess.run(cmd, cwd=DBT_DIR, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(
            f"dbt failed ({' '.join(args)}):\n{result.stdout[-2000:]}\n{result.stderr[-1000:]}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target", choices=config.TARGETS, required=True)
    parser.add_argument("--start", type=int, default=-96, help="first simulated load month")
    parser.add_argument("--end", type=int, default=-1, help="last simulated load month")
    parser.add_argument("--fresh", action="store_true", help="drop the snapshot first")
    args = parser.parse_args()
    if not -96 <= args.start <= args.end <= -1:
        sys.exit("Months must satisfy -96 <= start <= end <= -1")

    if args.fresh:
        run_dbt("run-operation", "drop_loan_status_snapshot", "--target", args.target)
    for month in range(args.start, args.end + 1):
        run_dbt(
            "snapshot",
            "--select",
            "snp_loan_status",
            "--target",
            args.target,
            "--vars",
            f"{{as_of_month: {month}}}",
        )
        print(f"simulated load as_of_month={month} done", flush=True)


if __name__ == "__main__":
    main()
