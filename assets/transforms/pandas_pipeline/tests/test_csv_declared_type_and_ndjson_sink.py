"""Tests for two gaps ported from `polars_pipeline`:

1. `kind: file`/`kind: url` CSV-only declared-type read options -- `has_header`,
   `quote_char`, `dtype_overrides` -- the gap against Azure Data Factory's Copy activity,
   whose declared-type source read can't be matched by pandas' own schema INFERENCE alone
   (a column of zero-padded codes like "001" silently becomes the integer 1).
2. `kind: ndjson` sink (pandas' native `to_json(orient="records", lines=True)` /
   `read_json(..., lines=True)`), closing the gap where JSON output otherwise needed a
   second asset even though the `encrypted_file` sink already supported `format: ndjson`.

Every test actually materializes the asset via dagster.materialize(), same convention as
test_pandas_pipeline.py.
"""
import json
import os

import dagster as dg
import pandas as pd
import pytest


def _materialize_file_source(mod, path, **source_overrides):
    comp = mod.PandasPipelineComponent(
        asset_name="out_asset",
        source={"kind": "file", "path": str(path), "format": "csv", **source_overrides},
        operations=[],
    )
    defs = comp.build_defs(None)
    result = dg.materialize([defs.get_assets_def(dg.AssetKey("out_asset"))])
    return result.output_for_node("out_asset")


def test_dtype_overrides_keeps_a_zero_padded_code_as_a_string(mod, tmp_path):
    """The exact failure this exists to fix: pandas' own default inference turns "001" into 1."""
    path = tmp_path / "codes.csv"
    path.write_text("account_code,amount\n001,10.5\n002,20.0\n")

    without_override = _materialize_file_source(mod, path)
    assert without_override["account_code"].tolist() == [1, 2]  # the bug, reproduced

    with_override = _materialize_file_source(mod, path, dtype_overrides={"account_code": "object"})
    assert with_override["account_code"].tolist() == ["001", "002"]
    assert with_override["amount"].tolist() == [10.5, 20.0]  # unlisted column still inferred normally


def test_dtype_overrides_accepts_the_nullable_string_dtype(mod, tmp_path):
    path = tmp_path / "codes.csv"
    path.write_text("account_code\n007\n")
    out = _materialize_file_source(mod, path, dtype_overrides={"account_code": "string"})
    assert out["account_code"].tolist() == ["007"]


def test_unknown_dtype_override_is_a_clear_error(mod, tmp_path):
    path = tmp_path / "codes.csv"
    path.write_text("account_code\n001\n")
    comp = mod.PandasPipelineComponent(
        asset_name="out_asset",
        source={
            "kind": "file", "path": str(path), "format": "csv",
            "dtype_overrides": {"account_code": "NotARealDtype"},
        },
        operations=[],
    )
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="unknown pandas dtype"):
        dg.materialize([defs.get_assets_def(dg.AssetKey("out_asset"))])


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
    assert out["note"].tolist() == ["a,b"]  # the quoted comma was not treated as a field separator


def test_default_behavior_unchanged_when_new_options_unset(mod, tmp_path):
    path = tmp_path / "plain.csv"
    path.write_text("id,amount\n1,10.5\n2,20.0\n")
    out = _materialize_file_source(mod, path)
    assert out["id"].tolist() == [1, 2]
    assert out["amount"].tolist() == [10.5, 20.0]


@dg.asset
def _raw_rows_for_ndjson():
    return pd.DataFrame({"a": [1, 2]})


def test_ndjson_sink(mod, tmp_path):
    path = str(tmp_path / "out.ndjson")
    comp = mod.PandasPipelineComponent(
        asset_name="ndjson_out",
        upstream_asset_key="_raw_rows_for_ndjson",
        operations=[],
        sinks=[{"from": "_default", "kind": "ndjson", "path": path}],
    )
    defs = dg.Definitions.merge(
        dg.Definitions(assets=[_raw_rows_for_ndjson]), comp.build_defs(None)
    )
    dg.materialize([_raw_rows_for_ndjson, defs.get_assets_def(dg.AssetKey("ndjson_out"))])

    assert os.path.exists(path)
    out = pd.read_json(path, orient="records", lines=True)
    assert out["a"].tolist() == [1, 2]
    # genuinely newline-delimited, one JSON object per line -- not a JSON array
    lines = [l for l in open(path).read().splitlines() if l.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}


def test_ndjson_append_mode(mod, tmp_path):
    path = str(tmp_path / "out.ndjson")

    def _comp():
        return mod.PandasPipelineComponent(
            asset_name="ndjson_append",
            upstream_asset_key="_raw_rows_for_ndjson",
            operations=[],
            sinks=[{"from": "_default", "kind": "ndjson", "path": path, "mode": "append"}],
        )

    defs = dg.Definitions.merge(
        dg.Definitions(assets=[_raw_rows_for_ndjson]), _comp().build_defs(None)
    )
    dg.materialize([_raw_rows_for_ndjson, defs.get_assets_def(dg.AssetKey("ndjson_append"))])
    defs2 = dg.Definitions.merge(
        dg.Definitions(assets=[_raw_rows_for_ndjson]), _comp().build_defs(None)
    )
    dg.materialize([_raw_rows_for_ndjson, defs2.get_assets_def(dg.AssetKey("ndjson_append"))])

    assert len(pd.read_json(path, orient="records", lines=True)) == 4


def test_ndjson_sink_invalid_mode_raises(mod, tmp_path):
    path = str(tmp_path / "out.ndjson")
    comp = mod.PandasPipelineComponent(
        asset_name="ndjson_bad_mode",
        upstream_asset_key="_raw_rows_for_ndjson",
        operations=[],
        sinks=[{"from": "_default", "kind": "ndjson", "path": path, "mode": "not_a_real_mode"}],
    )
    defs = dg.Definitions.merge(
        dg.Definitions(assets=[_raw_rows_for_ndjson]), comp.build_defs(None)
    )
    with pytest.raises(Exception, match="mode must be"):
        dg.materialize([_raw_rows_for_ndjson, defs.get_assets_def(dg.AssetKey("ndjson_bad_mode"))])
