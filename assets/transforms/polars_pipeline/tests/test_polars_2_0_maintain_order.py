"""Real, committed test for a Polars 2.0 gap: polars' streaming query engine
(default since 2.0) doesn't guarantee group_by/join output row order unless
`maintain_order` is set -- a silent reordering, not an error. Confirmed
against a real, installed polars 2.0.0.

group_by matters here specifically because this pipeline's own `head`/
`limit` step can run right after a `group_by` step in the same
config-driven chain (`{op: head, n: N}` taking "the first N groups"),
which would be nondeterministic without it.
"""
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg


@pytest.fixture()
def mod():
    return load_component_module()


def test_group_by_then_head_is_deterministic_across_runs(mod):
    """Confirms group_by(maintain_order=True) -- run the same pipeline twice
    against the same input and require the exact same 'first 2 groups'."""

    @dg.asset(name="raw_orders")
    def raw_orders():
        return pd.DataFrame({
            "region": ["east", "west", "north", "south", "east", "west"],
            "amount": [10, 20, 30, 40, 5, 6],
        })

    def _run():
        comp = mod.PolarsPipelineComponent(
            asset_name="out_asset",
            upstream_asset_key="raw_orders",
            operations=[
                {"op": "group_by", "group_by": ["region"],
                 "aggregations": {"revenue": {"col": "amount", "agg": "sum"}}},
                {"op": "head", "n": 2},
            ],
        )
        comp_assets = comp.build_defs(None).assets
        result = dg.materialize(
            [raw_orders] + list(comp_assets),
            resources={"io_manager": dg.InMemoryIOManager()},
        )
        assert result.success
        return result.output_for_node("out_asset")

    first = _run()
    second = _run()
    assert list(first["region"]) == list(second["region"])
    # The real, substantive claim: the first 2 groups are a stable, repeatable
    # subset of the input's own group order, not whatever the engine felt like.
    assert list(first["region"]) == ["east", "west"]
