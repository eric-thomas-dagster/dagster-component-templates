"""Tests for PolarsPipelineComponent's `kind: file`/`kind: url` CSV-only declared-type read options:
`has_header`, `quote_char`, `schema_overrides` -- the gap against Azure Data Factory's Copy activity,
whose declared-type source read can't be matched by polars' own schema INFERENCE alone (a column of
zero-padded codes like "001" silently becomes the integer 1).
"""
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg
import polars as pl


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_file_source(mod, path, **source_overrides):
    comp = mod.PolarsPipelineComponent(
        asset_name="out_asset",
        upstream_asset_key=None,
        source={"kind": "file", "path": str(path), "format": "csv", **source_overrides},
        operations=[],
    )
    defs = comp.build_defs(context=None)
    result = dg.materialize(defs.assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    return result.asset_value(dg.AssetKey("out_asset"))


def test_schema_overrides_keeps_a_zero_padded_code_as_a_string(mod, tmp_path):
    """The exact failure this exists to fix: polars' own default inference turns "001" into 1."""
    path = tmp_path / "codes.csv"
    path.write_text("account_code,amount\n001,10.5\n002,20.0\n")

    without_override = _materialize_file_source(mod, path)
    assert without_override["account_code"].to_list() == [1, 2]  # the bug, reproduced

    with_override = _materialize_file_source(mod, path, schema_overrides={"account_code": "Utf8"})
    assert with_override["account_code"].to_list() == ["001", "002"]
    assert with_override["amount"].to_list() == [10.5, 20.0]  # unlisted column still inferred normally


def test_schema_overrides_accepts_the_string_dtype_alias(mod, tmp_path):
    path = tmp_path / "codes.csv"
    path.write_text("account_code\n007\n")
    out = _materialize_file_source(mod, path, schema_overrides={"account_code": "String"})
    assert out["account_code"].to_list() == ["007"]


def test_unknown_schema_override_dtype_is_a_clear_error(mod, tmp_path):
    path = tmp_path / "codes.csv"
    path.write_text("account_code\n001\n")
    comp = mod.PolarsPipelineComponent(
        asset_name="out_asset",
        source={"kind": "file", "path": str(path), "format": "csv",
                "schema_overrides": {"account_code": "NotARealDtype"}},
        operations=[],
    )
    defs = comp.build_defs(context=None)
    with pytest.raises(Exception, match="unknown polars dtype"):
        dg.materialize(defs.assets, resources={"io_manager": dg.InMemoryIOManager()})


def test_has_header_false_reads_a_headerless_file(mod, tmp_path):
    path = tmp_path / "headerless.csv"
    path.write_text("1,10.5\n2,20.0\n")
    out = _materialize_file_source(mod, path, has_header=False)
    assert len(out) == 2
    assert len(out.columns) == 2


def test_quote_char_handles_a_non_default_quote_character(mod, tmp_path):
    path = tmp_path / "quoted.csv"
    path.write_text("id,note\n1,'a,b'\n")
    out = _materialize_file_source(mod, path, quote_char="'")
    assert out["note"].to_list() == ["a,b"]  # the quoted comma was not treated as a field separator


def test_default_behavior_unchanged_when_new_options_unset(mod, tmp_path):
    path = tmp_path / "plain.csv"
    path.write_text("id,amount\n1,10.5\n2,20.0\n")
    out = _materialize_file_source(mod, path)
    assert out["id"].to_list() == [1, 2]
    assert out["amount"].to_list() == [10.5, 20.0]
