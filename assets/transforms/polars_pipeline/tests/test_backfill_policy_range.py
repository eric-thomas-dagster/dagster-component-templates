"""polars_pipeline's backfill_policy was missing entirely (field + wiring), and its existing
partition_key templating only handled a single key (`context.partition_key if context.has_partition_key
else None`) -- a single_run backfill hands the asset a partition-key RANGE, which would have silently
produced partition_key=None instead of a real range representation. Real dg.materialize() run; the only
thing standing in for an external system is a local parquet file sink (no network boundary here to mock)."""
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import dagster as dg


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, tmp_path, backfill_policy=None):
    return mod.PolarsPipelineComponent(
        asset_name="out_asset",
        upstream_asset_key="raw_orders",
        operations=[{"op": "select", "columns": ["region", "amount"]}],
        sinks=[{"from": "_default", "kind": "parquet", "path": str(tmp_path / "{partition_key}.parquet")}],
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )


def _upstream_asset():
    @dg.asset(name="raw_orders")
    def raw_orders():
        return pd.DataFrame({"region": ["east", "west"], "amount": [10, 20]})
    return raw_orders


def test_backfill_policy_single_run_sets_a_real_backfill_policy(mod, tmp_path):
    comp = _comp(mod, tmp_path, backfill_policy="single_run")
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_joins_partition_keys_into_one_sink_path(mod, tmp_path):
    comp = _comp(mod, tmp_path, backfill_policy="single_run")
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))

    result = dg.materialize(
        [asset, _upstream_asset()],
        tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-02",
        },
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    assert (tmp_path / "2024-01-01,2024-01-02.parquet").exists()


def test_a_single_partition_tick_still_uses_just_that_one_key(mod, tmp_path):
    """Confirms the fix didn't regress the normal (non-backfill) single-key path."""
    comp = _comp(mod, tmp_path, backfill_policy="single_run")
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))

    result = dg.materialize([asset, _upstream_asset()], partition_key="2024-01-01")
    assert result.success
    assert (tmp_path / "2024-01-01.parquet").exists()
