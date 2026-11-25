"""Committed tests for TextToColumns's on_insufficient_parts option.

StreamSets' Splitter treats a too-short value as a record error rather than
padding missing columns with null. Real dg.materialize() runs, nothing mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, **overrides):
    @dg.asset(name="src")
    def src():
        return pd.DataFrame({"text": ["a,b,c", "x,y", "p,q,r"]})

    cls = getattr(mod, "TextToColumns")
    comp = cls(
        asset_name="out_asset",
        upstream_asset_key="src",
        column="text",
        separator=",",
        output_columns=["a", "b", "c"],
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


def test_default_pad_matches_prior_behavior(mod):
    """Default (unset) stays 'pad' — backward compatible, no behavior change."""
    out = _materialize(mod)
    assert out["c"].isna().sum() == 1  # the "x,y" row is short by one part


def test_on_insufficient_parts_error_raises(mod):
    cls = getattr(mod, "TextToColumns")
    comp = cls(
        asset_name="out_asset",
        upstream_asset_key="src",
        column="text",
        separator=",",
        output_columns=["a", "b", "c"],
        on_insufficient_parts="error",
    )

    @dg.asset(name="src")
    def src():
        return pd.DataFrame({"text": ["a,b,c", "x,y", "p,q,r"]})

    comp_assets = comp.build_defs(None).assets
    with pytest.raises(ValueError, match="fewer than"):
        dg.materialize(
            [src] + list(comp_assets),
            resources={"io_manager": dg.InMemoryIOManager()},
        )


def test_on_insufficient_parts_error_passes_when_all_rows_sufficient(mod):
    """No short rows at all -> 'error' mode never fires, same output as 'pad'."""
    cls = getattr(mod, "TextToColumns")

    @dg.asset(name="src")
    def src():
        return pd.DataFrame({"text": ["a,b,c", "x,y,z", "p,q,r"]})

    comp = cls(
        asset_name="out_asset",
        upstream_asset_key="src",
        column="text",
        separator=",",
        output_columns=["a", "b", "c"],
        on_insufficient_parts="error",
    )
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [src] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
