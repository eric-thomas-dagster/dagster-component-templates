"""Committed tests for DataframeFlattenNestedColumnsComponent's
streamsets_list_index_naming option.

StreamSets' Field Flattener names auto-generated list-index columns with a
literal `l` segment (e.g. `coordinates.l.0`) rather than a bare integer.
Real dg.materialize() runs.
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

    cls = getattr(mod, "DataframeFlattenNestedColumnsComponent")
    comp = cls(
        asset_name="out_asset",
        upstream_asset_key="src",
        expand=True,
        expand_arrays=True,
        **overrides,
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [src] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    out = result.output_for_node("out_asset")
    return out.value if hasattr(out, "value") and hasattr(out, "metadata") else out


def test_default_bare_integer_naming(mod):
    out = _materialize(mod, [{"coordinates": [1.0, 2.0, 3.0]}])
    assert "coordinates.0" in out.columns
    assert "coordinates.1" in out.columns
    assert "coordinates.l.0" not in out.columns


def test_streamsets_naming_uses_l_prefix(mod):
    out = _materialize(mod, [{"coordinates": [1.0, 2.0, 3.0]}], streamsets_list_index_naming=True)
    assert "coordinates.l.0" in out.columns
    assert "coordinates.l.1" in out.columns
    assert "coordinates.0" not in out.columns


def test_explicit_array_label_map_wins_over_streamsets_naming(mod):
    """An explicit label always wins, regardless of streamsets_list_index_naming."""
    out = _materialize(
        mod,
        [{"coordinates": [1.0, 2.0, 3.0]}],
        streamsets_list_index_naming=True,
        array_label_map={"coordinates": ["lon", "lat", "depth"]},
    )
    assert "coordinates.lon" in out.columns
    assert "coordinates.l.0" not in out.columns


def test_streamsets_naming_applies_to_nested_second_pass_lists(mod):
    """Second-pass list splitting (a list inside an expanded dict column) also
    respects streamsets_list_index_naming, not just the first-pass one."""
    out = _materialize(
        mod,
        [{"geometry": {"coordinates": [10.0, 20.0]}}],
        streamsets_list_index_naming=True,
    )
    assert "geometry.coordinates.l.0" in out.columns
    assert "geometry.coordinates.l.1" in out.columns
