"""Tests for json_to_iceberg.schema."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from json_to_iceberg.schema import (
    _resolve_dtype,
    dataframe_to_arrow,
    resolve_null_types,
)


# ── _resolve_dtype ──────────────────────────────────────────────────────────


class TestResolveDtype:
    def test_null_becomes_default(self):
        assert _resolve_dtype(pl.Null, pl.Utf8) == pl.Utf8

    def test_concrete_type_unchanged(self):
        assert _resolve_dtype(pl.Int64, pl.Utf8) == pl.Int64

    def test_list_of_null(self):
        assert _resolve_dtype(pl.List(pl.Null), pl.Utf8) == pl.List(pl.Utf8)

    def test_list_of_concrete_unchanged(self):
        assert _resolve_dtype(pl.List(pl.Float64), pl.Utf8) == pl.List(pl.Float64)

    def test_struct_with_null_field(self):
        dtype = pl.Struct([pl.Field("a", pl.Null), pl.Field("b", pl.Int64)])
        expected = pl.Struct([pl.Field("a", pl.Utf8), pl.Field("b", pl.Int64)])
        assert _resolve_dtype(dtype, pl.Utf8) == expected

    def test_nested_list_struct_null(self):
        dtype = pl.List(pl.Struct([pl.Field("x", pl.Null)]))
        expected = pl.List(pl.Struct([pl.Field("x", pl.Utf8)]))
        assert _resolve_dtype(dtype, pl.Utf8) == expected


# ── resolve_null_types ──────────────────────────────────────────────────────


class TestResolveNullTypes:
    def test_all_null_column_becomes_utf8(self):
        df = pl.DataFrame({"a": [None, None, None]})
        assert df.schema["a"] == pl.Null

        resolved = resolve_null_types(df)
        assert resolved.schema["a"] == pl.Utf8
        assert resolved["a"].to_list() == [None, None, None]

    def test_concrete_columns_unchanged(self):
        df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        resolved = resolve_null_types(df)
        assert resolved.schema == df.schema
        assert resolved.equals(df)

    def test_mixed_null_and_concrete(self):
        df = pl.DataFrame(
            {"ok": [1, 2], "bad": [None, None]},
            schema={"ok": pl.Int64, "bad": pl.Null},
        )
        resolved = resolve_null_types(df)
        assert resolved.schema["ok"] == pl.Int64
        assert resolved.schema["bad"] == pl.Utf8


# ── dataframe_to_arrow ─────────────────────────────────────────────────────


class TestDataframeToArrow:
    def test_basic_conversion(self):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        table = dataframe_to_arrow(df)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.schema.field("a").type == pa.int64()
        assert table.schema.field("b").type == pa.large_utf8()

    def test_nanosecond_downcast(self):
        from datetime import datetime

        df = pl.DataFrame({"ts": [datetime(2024, 1, 1)]})
        # Polars stores datetimes as us by default, but let's test the path
        table = dataframe_to_arrow(df)
        ts_type = table.schema.field("ts").type
        assert pa.types.is_timestamp(ts_type)
        assert ts_type.unit in ("us", "ms")  # must not be 'ns'
