"""Tests for MLPipelineComponent's `kind: file`/`kind: url` CSV-only declared-type
read options: `header`, `quotechar`, `dtype_overrides` -- the same gap closed in
polars_pipeline (commit 119035e73) against Azure Data Factory's Copy activity,
whose declared-type source read can't be matched by pandas' own schema
INFERENCE alone (a column of zero-padded codes like "001" silently becomes the
integer 1).
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize_file_source(mod, path, prefix, **source_overrides):
    component = mod.MLPipelineComponent(
        asset_name_prefix=prefix,
        source={"kind": "file", "path": str(path), **source_overrides},
        target_column="amount",
        feature_columns=["amount"],
        steps=[
            # `select` requires an explicit column list; use `rename` with an
            # empty mapping as a pure pass-through of the ingested frame.
            {"id": "ingested", "op": "rename", "columns": {}},
        ],
        outputs={"assets": ["ingested"]},
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    return result.output_for_node(f"{prefix}_pipeline", output_name=f"{prefix}_ingested")


def test_dtype_overrides_keeps_a_zero_padded_code_as_a_string(mod, tmp_path):
    """The exact failure this exists to fix: pandas' own default inference turns "001" into 1."""
    path = tmp_path / "codes.csv"
    path.write_text("account_code,amount\n001,10.5\n002,20.0\n")

    without_override = _materialize_file_source(mod, path, "codesplain")
    assert list(without_override["account_code"]) == [1, 2]  # the bug, reproduced

    with_override = _materialize_file_source(
        mod, path, "codesfixed", dtype_overrides={"account_code": "object"},
    )
    assert list(with_override["account_code"]) == ["001", "002"]
    assert list(with_override["amount"]) == [10.5, 20.0]  # unlisted column still inferred normally


def test_header_none_reads_a_headerless_file(mod, tmp_path):
    path = tmp_path / "headerless.csv"
    path.write_text("1,10.5\n2,20.0\n")
    out = _materialize_file_source(mod, path, "headerless", header=None)
    assert len(out) == 2
    assert len(out.columns) == 2
    # Without a header row, pandas assigns positional integer column names (0, 1).
    assert list(out.columns) == [0, 1]


def test_quotechar_handles_a_non_default_quote_character(mod, tmp_path):
    path = tmp_path / "quoted.csv"
    path.write_text("amount,note\n10.5,'a,b'\n")
    out = _materialize_file_source(mod, path, "quoted", quotechar="'")
    assert list(out["note"]) == ["a,b"]  # the quoted comma was not treated as a field separator


def test_default_behavior_unchanged_when_new_options_unset(mod, tmp_path):
    path = tmp_path / "plain.csv"
    path.write_text("id,amount\n1,10.5\n2,20.0\n")
    out = _materialize_file_source(mod, path, "plain")
    assert list(out["id"]) == [1, 2]
    assert list(out["amount"]) == [10.5, 20.0]
