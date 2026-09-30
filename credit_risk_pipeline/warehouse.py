"""Warehouse helpers shared by the loader scripts: identifier checks and SQL literals."""

from __future__ import annotations

import re

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_identifier(name: str) -> str:
    """Reject anything that is not a plain SQL identifier (column names come from CSV headers)."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe or unsupported identifier in source header: {name!r}")
    return name


def sql_string(value: str) -> str:
    """Quote a string literal for SQL."""
    return "'" + value.replace("'", "''") + "'"
