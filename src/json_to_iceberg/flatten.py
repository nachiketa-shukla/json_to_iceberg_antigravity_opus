"""Recursive JSON flattening for nested SaaS API responses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def flatten_record(
    record: dict[str, Any],
    separator: str = ".",
    _prefix: str = "",
) -> dict[str, Any]:
    """Flatten a single nested dict into a flat dict with dot-delimited keys.

    - Nested objects  → flattened keys (e.g. ``user.address.city``)
    - Arrays of scalars → kept as Python lists (→ Polars ``List`` type)
    - Arrays of objects → kept as lists of dicts (→ Polars ``List(Struct)`` type)
    - ``None`` values → preserved for schema inference
    """
    flat: dict[str, Any] = {}
    for key, value in record.items():
        full_key = f"{_prefix}{separator}{key}" if _prefix else key
        if isinstance(value, dict):
            flat.update(flatten_record(value, separator, full_key))
        else:
            # Scalars, None, lists of scalars, or lists of dicts — keep as-is
            flat[full_key] = value
    return flat


def _detect_records(raw: Any) -> list[dict[str, Any]]:
    """Auto-detect the records array from a parsed JSON payload.

    Supports:
    - Top-level array:  ``[{...}, {...}]``
    - Wrapped:          ``{"data": [{...}], ...}``  (first array-of-dicts key)
    - Single object:    ``{...}`` (treated as one-record list)
    """
    if isinstance(raw, list):
        return raw

    if isinstance(raw, dict):
        # Look for the first value that is a non-empty list of dicts
        for value in raw.values():
            if (
                isinstance(value, list)
                and value
                and isinstance(value[0], dict)
            ):
                return value
        # Fallback: treat the whole dict as a single record
        return [raw]

    raise ValueError(f"Unexpected JSON root type: {type(raw).__name__}")


def load_and_flatten(
    path: Path,
    separator: str = ".",
) -> pl.DataFrame:
    """Read a JSON file, flatten nested records, and return a Polars DataFrame.

    Args:
        path: Path to a JSON file.
        separator: Delimiter for nested key names.

    Returns:
        A Polars DataFrame with flattened columns.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    records = _detect_records(raw)
    flat_records = [flatten_record(r, separator) for r in records]

    return pl.DataFrame(flat_records)
