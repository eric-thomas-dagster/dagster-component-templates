"""Tests for FieldMergerComponent -- StreamSets Field Merger semantics: source field is MOVED (dropped
from output), lists append, maps raise on a conflicting key unless on_key_conflict='overwrite',
and mismatched/non-list-non-map types are a clear error."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _upstream(df, name="upstream"):
    @dg.asset(name=name)
    def _asset():
        return df

    return _asset


def _materialize(mod, df, **kwargs):
    kwargs.setdefault("asset_name", "out_asset")
    kwargs["upstream_asset_key"] = "upstream"
    comp = mod.FieldMergerComponent(**kwargs)
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_upstream(df), *defs.assets])
    assert result.success
    return result.asset_value("out_asset")


def test_lists_are_appended_target_then_source(mod):
    df = pd.DataFrame({
        "id": [1],
        "extra_tags": [["c", "d"]],
        "tags": [["a", "b"]],
    })
    out = _materialize(mod, df, source_field="extra_tags", target_field="tags")
    assert out.iloc[0]["tags"] == ["a", "b", "c", "d"]


def test_source_field_is_dropped_from_output(mod):
    df = pd.DataFrame({"id": [1], "extra_tags": [["c"]], "tags": [["a"]]})
    out = _materialize(mod, df, source_field="extra_tags", target_field="tags")
    assert "extra_tags" not in out.columns


def test_maps_merge_without_conflicting_keys(mod):
    df = pd.DataFrame({
        "id": [1],
        "extra": [{"b": 2}],
        "base": [{"a": 1}],
    })
    out = _materialize(mod, df, source_field="extra", target_field="base")
    assert out.iloc[0]["base"] == {"a": 1, "b": 2}


def test_maps_conflicting_key_raises_by_default(mod):
    df = pd.DataFrame({
        "id": [1],
        "extra": [{"a": 99}],
        "base": [{"a": 1}],
    })
    result = dg.materialize(
        [_upstream(df), *mod.FieldMergerComponent(
            asset_name="out_asset", upstream_asset_key="upstream",
            source_field="extra", target_field="base",
        ).build_defs(load_context=None).assets],
        raise_on_error=False,
    )
    assert not result.success


def test_maps_conflicting_key_overwrite_lets_source_win(mod):
    df = pd.DataFrame({
        "id": [1],
        "extra": [{"a": 99}],
        "base": [{"a": 1}],
    })
    out = _materialize(mod, df, source_field="extra", target_field="base", on_key_conflict="overwrite")
    assert out.iloc[0]["base"] == {"a": 99}


def test_mismatched_types_raises(mod):
    df = pd.DataFrame({"id": [1], "extra": [{"a": 1}], "base": [["x"]]})
    result = dg.materialize(
        [_upstream(df), *mod.FieldMergerComponent(
            asset_name="out_asset", upstream_asset_key="upstream",
            source_field="extra", target_field="base",
        ).build_defs(load_context=None).assets],
        raise_on_error=False,
    )
    assert not result.success


def test_scalar_values_raise(mod):
    df = pd.DataFrame({"id": [1], "extra": [5], "base": [["x"]]})
    result = dg.materialize(
        [_upstream(df), *mod.FieldMergerComponent(
            asset_name="out_asset", upstream_asset_key="upstream",
            source_field="extra", target_field="base",
        ).build_defs(load_context=None).assets],
        raise_on_error=False,
    )
    assert not result.success
