"""Tests for json_to_iceberg.flatten."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import polars as pl
import pytest

from json_to_iceberg.flatten import (
    _detect_records,
    flatten_record,
    load_and_flatten,
)


# ── flatten_record ──────────────────────────────────────────────────────────


class TestFlattenRecord:
    def test_flat_object(self):
        assert flatten_record({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}

    def test_single_level_nesting(self):
        result = flatten_record({"user": {"name": "Alice", "age": 30}})
        assert result == {"user.name": "Alice", "user.age": 30}

    def test_deep_nesting(self):
        record = {"a": {"b": {"c": {"d": 42}}}}
        assert flatten_record(record) == {"a.b.c.d": 42}

    def test_custom_separator(self):
        result = flatten_record({"a": {"b": 1}}, separator="__")
        assert result == {"a__b": 1}

    def test_preserves_none(self):
        result = flatten_record({"x": None, "y": {"z": None}})
        assert result == {"x": None, "y.z": None}

    def test_preserves_scalar_list(self):
        result = flatten_record({"tags": ["a", "b"]})
        assert result == {"tags": ["a", "b"]}

    def test_preserves_list_of_dicts(self):
        records = [{"id": 1}, {"id": 2}]
        result = flatten_record({"items": records})
        assert result == {"items": [{"id": 1}, {"id": 2}]}

    def test_mixed_nesting(self):
        record = {
            "id": 1,
            "meta": {"name": "test"},
            "tags": [1, 2],
        }
        result = flatten_record(record)
        assert result == {"id": 1, "meta.name": "test", "tags": [1, 2]}

    def test_empty_record(self):
        assert flatten_record({}) == {}


# ── _detect_records ─────────────────────────────────────────────────────────


class TestDetectRecords:
    def test_top_level_array(self):
        assert _detect_records([{"a": 1}]) == [{"a": 1}]

    def test_wrapped_data_key(self):
        raw = {"data": [{"a": 1}], "meta": {"page": 1}}
        assert _detect_records(raw) == [{"a": 1}]

    def test_single_object(self):
        assert _detect_records({"a": 1}) == [{"a": 1}]

    def test_non_standard_wrapper_key(self):
        raw = {"results": [{"id": 1}], "count": 1}
        assert _detect_records(raw) == [{"id": 1}]

    def test_raises_on_bad_type(self):
        with pytest.raises(ValueError, match="Unexpected JSON root type"):
            _detect_records("not json")


# ── load_and_flatten ────────────────────────────────────────────────────────


class TestLoadAndFlatten:
    def test_loads_sample_fixture(self):
        fixture = Path(__file__).resolve().parent.parent / "fixtures" / "sample_api_response.json"
        if not fixture.exists():
            pytest.skip("fixture not found")

        df = load_and_flatten(fixture)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 3
        # Deeply nested field should be flattened
        assert "profile.address.geo.lat" in df.columns

    def test_roundtrip_from_tmp(self, tmp_path: Path):
        data = [{"x": 1, "nested": {"y": 2}}, {"x": 3, "nested": {"y": 4}}]
        p = tmp_path / "test.json"
        p.write_text(json.dumps(data))

        df = load_and_flatten(p)
        assert df.height == 2
        assert set(df.columns) == {"x", "nested.y"}
        assert df["x"].to_list() == [1, 3]
