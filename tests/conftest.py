"""Test fixtures. All data here is SYNTHETIC and exists only to exercise the code paths.
It must never be copied into data/sample/ or used for any analysis."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from credit_risk_pipeline.tables import TABLES_BY_NAME  # noqa: E402

N_CLIENTS = 40


def _write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    lines = [",".join(header)] + [",".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def synthetic_raw(tmp_path: Path) -> Path:
    """Tiny fake versions of the 7 Home Credit files (SYNTHETIC, not real data)."""
    raw = tmp_path / "raw"
    raw.mkdir()
    t = TABLES_BY_NAME
    clients = [str(100000 + i) for i in range(N_CLIENTS)]
    _write(
        raw / t["application_train"].csv_file,
        ["SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL"],
        [[c, str(i % 5 == 0 and 1 or 0), ""] for i, c in enumerate(clients)],
    )
    bureau = [[c, str(500000 + i * 10 + k)] for i, c in enumerate(clients) for k in range(2)]
    _write(raw / t["bureau"].csv_file, ["SK_ID_CURR", "SK_ID_BUREAU"], bureau)
    _write(
        raw / t["bureau_balance"].csv_file,
        ["SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"],
        [[b[1], str(-m), "0"] for b in bureau for m in range(3)],
    )
    prev = [[c, str(200000 + i)] for i, c in enumerate(clients)]
    _write(raw / t["previous_application"].csv_file, ["SK_ID_CURR", "SK_ID_PREV"], prev)
    _write(
        raw / t["installments_payments"].csv_file,
        ["SK_ID_PREV", "SK_ID_CURR", "NUM_INSTALMENT_NUMBER"],
        [[p[1], p[0], str(n)] for p in prev for n in range(2)],
    )
    _write(
        raw / t["pos_cash_balance"].csv_file,
        ["SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"],
        [[p[1], p[0], str(-n)] for p in prev for n in range(2)],
    )
    _write(
        raw / t["credit_card_balance"].csv_file,
        ["SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"],
        [[p[1], p[0], str(-n)] for p in prev[:10] for n in range(2)],
    )
    return raw
