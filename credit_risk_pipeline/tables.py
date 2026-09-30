"""Registry of the Home Credit source tables that land in the RAW layer."""

from __future__ import annotations

from dataclasses import dataclass

COMPETITION = "home-credit-default-risk"


@dataclass(frozen=True)
class RawTable:
    """One source file and the RAW table it is loaded into."""

    name: str  # RAW table name, lower case (Snowflake folds it to upper case)
    csv_file: str  # file name inside the Kaggle competition
    grain: str  # documented grain, used in docs and sanity checks
    required_columns: tuple[str, ...]  # header sanity check; also the sampling join keys

    @property
    def sample_file(self) -> str:
        return f"{self.name}.parquet"


RAW_TABLES: tuple[RawTable, ...] = (
    RawTable(
        "application_train",
        "application_train.csv",
        "one row per loan application (SK_ID_CURR)",
        ("SK_ID_CURR", "TARGET"),
    ),
    RawTable(
        "bureau",
        "bureau.csv",
        "one row per credit reported to the bureau (SK_ID_BUREAU)",
        ("SK_ID_CURR", "SK_ID_BUREAU"),
    ),
    RawTable(
        "bureau_balance",
        "bureau_balance.csv",
        "one row per bureau credit per month (SK_ID_BUREAU, MONTHS_BALANCE)",
        ("SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"),
    ),
    RawTable(
        "previous_application",
        "previous_application.csv",
        "one row per previous Home Credit application (SK_ID_PREV)",
        ("SK_ID_PREV", "SK_ID_CURR"),
    ),
    RawTable(
        "installments_payments",
        "installments_payments.csv",
        "one row per installment payment (SK_ID_PREV, installment number/version)",
        ("SK_ID_PREV", "SK_ID_CURR"),
    ),
    RawTable(
        "pos_cash_balance",
        "POS_CASH_balance.csv",
        "one row per POS/cash loan per month (SK_ID_PREV, MONTHS_BALANCE)",
        ("SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"),
    ),
    RawTable(
        "credit_card_balance",
        "credit_card_balance.csv",
        "one row per credit card per month (SK_ID_PREV, MONTHS_BALANCE)",
        ("SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"),
    ),
)

TABLES_BY_NAME = {t.name: t for t in RAW_TABLES}

# Audit columns appended to every RAW table.
AUDIT_COLUMNS = ("_loaded_at", "_source_file")
