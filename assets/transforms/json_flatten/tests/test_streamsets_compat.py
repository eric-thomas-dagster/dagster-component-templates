"""Committed tests for JsonFlattenComponent's flatten_mode + expand_lists options.

StreamSets' Field Flattener (1) expands list elements the same way it expands
nested dicts (keyed l.<index>), and (2) in ROOT mode drops the source field's
own name from the resulting key paths. Real dg.materialize() runs.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, rows, **overrides):
    @dg.asset(name="src")
    def src():
        return pd.DataFrame(rows)

    cls = getattr(mod, "JsonFlattenComponent")
    comp = cls(asset_name="out_asset", upstream_asset_key="src", column="payload", **overrides)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [src] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    out = result.output_for_node("out_asset")
    return out.value if hasattr(out, "value") and hasattr(out, "metadata") else out


def test_default_in_place_keeps_prefix(mod):
    out = _materialize(mod, [{"payload": {"city": "Chicago"}}])
    assert "payload.city" in out.columns
    assert "city" not in out.columns


def test_root_mode_drops_prefix(mod):
    out = _materialize(mod, [{"payload": {"city": "Chicago"}}], flatten_mode="root")
    assert "city" in out.columns
    assert "payload.city" not in out.columns


def test_expand_lists_false_leaves_lists_opaque(mod):
    out = _materialize(mod, [{"payload": {"phones": [{"number": "555"}, {"number": "666"}]}}])
    assert "payload.phones" in out.columns
    assert isinstance(out["payload.phones"].iloc[0], list)


def test_expand_lists_true_expands_with_l_index(mod):
    out = _materialize(
        mod,
        [{"payload": {"phones": [{"number": "555"}, {"number": "666"}]}}],
        expand_lists=True,
    )
    assert "payload.phones.l.0.number" in out.columns
    assert "payload.phones.l.1.number" in out.columns
    assert out["payload.phones.l.0.number"].iloc[0] == "555"
    assert out["payload.phones.l.1.number"].iloc[0] == "666"


def test_expand_lists_and_root_mode_combine(mod):
    out = _materialize(
        mod,
        [{"payload": {"phones": [{"number": "555"}]}}],
        expand_lists=True,
        flatten_mode="root",
    )
    assert "phones.l.0.number" in out.columns
