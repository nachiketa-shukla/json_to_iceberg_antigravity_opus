"""Schema extraction, null-type resolution, and Polars ↔ Iceberg conversion."""

from __future__ import annotations

import pyarrow as pa
import polars as pl


# ---------------------------------------------------------------------------
# Null-type resolution
# ---------------------------------------------------------------------------

_DEFAULT_TYPE = pl.Utf8  # fallback for columns that are all-null


def resolve_null_types(
    df: pl.DataFrame,
    default: pl.DataType = _DEFAULT_TYPE,
) -> pl.DataFrame:
    """Return a new DataFrame with every ``Null``-typed column cast to *default*.

    This is necessary because Iceberg (and Parquet) do not support a
    pure ``null`` type — every column must have a concrete type.

    For ``List(Null)`` columns the inner type is replaced, producing
    ``List(default)``.  ``Struct`` fields are resolved recursively via
    the PyArrow bridge.
    """
    casts: dict[str, pl.DataType] = {}
    for name, dtype in df.schema.items():
        resolved = _resolve_dtype(dtype, default)
        if resolved != dtype:
            casts[name] = resolved

    if not casts:
        return df

    return df.with_columns(
        [pl.col(name).cast(dtype) for name, dtype in casts.items()]
    )


def _resolve_dtype(dtype: pl.DataType, default: pl.DataType) -> pl.DataType:
    """Recursively replace ``Null`` with *default* inside any Polars dtype."""
    if dtype == pl.Null:
        return default
    if isinstance(dtype, pl.List):
        inner = _resolve_dtype(dtype.inner, default)  # type: ignore[arg-type]
        return pl.List(inner)
    if isinstance(dtype, pl.Struct):
        fields = [
            pl.Field(f.name, _resolve_dtype(f.dtype, default))
            for f in dtype.fields
        ]
        return pl.Struct(fields)
    return dtype


# ---------------------------------------------------------------------------
# Polars → Arrow → Iceberg schema
# ---------------------------------------------------------------------------


def dataframe_to_arrow(df: pl.DataFrame) -> pa.Table:
    """Convert a *null-resolved* Polars DataFrame to a PyArrow Table.

    Timestamps are down-cast to microsecond precision so that PyIceberg
    (which doesn't support nanoseconds) can consume them.
    """
    arrow_table = df.to_arrow()

    # Down-cast any nanosecond timestamp columns to microseconds
    new_fields: list[pa.Field] = []
    needs_cast = False
    for field in arrow_table.schema:
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            new_fields.append(field.with_type(pa.timestamp("us", tz=field.type.tz)))
            needs_cast = True
        else:
            new_fields.append(field)

    if needs_cast:
        arrow_table = arrow_table.cast(pa.schema(new_fields))

    return arrow_table
