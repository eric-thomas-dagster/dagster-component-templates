"""warehouse_pipeline's backfill_policy was missing entirely in BOTH of its two asset-building paths
(single-sink @asset and the can_subset multi-sink @multi_asset), and its partition_key computation only
handled a single key -- a single_run backfill hands the asset a partition-key RANGE, which would have
silently produced partition_key=None instead of a real range representation (now a comma-joined range
string). This component compiles to raw SQL executed against an arbitrary warehouse resource, which this
test suite has no fixture for; build_defs() itself needs no connection, so this covers the field/wiring
surface -- including BOTH asset-building paths -- for real without inventing a fake warehouse connection."""
from .conftest import load_component_module

import dagster as dg


def _single_sink_comp(mod, backfill_policy=None):
    return mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        source={"upstream_table": "raw.orders"},
        operations=[{"op": "limit", "n": 10}],
        output_table="analytics.out",
        mode="replace",
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )


def _multi_sink_comp(mod, backfill_policy=None):
    return mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        steps=[{"id": "s1", "source": {"upstream_table": "raw.orders"}, "operations": [{"op": "limit", "n": 10}]}],
        sinks=[
            {"from": "s1", "table": "analytics.out1", "mode": "replace"},
            {"from": "s1", "table": "analytics.out2", "mode": "replace"},
        ],
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )


def test_single_sink_path_wires_backfill_policy():
    mod = load_component_module()
    comp = _single_sink_comp(mod, backfill_policy="single_run")
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_multi_sink_can_subset_path_also_wires_backfill_policy():
    """The multi-sink path is a SEPARATE _build_multi_asset_defs() code path -- confirms the fix
    threaded backfill_policy through both, not just the single-sink one."""
    mod = load_component_module()
    comp = _multi_sink_comp(mod, backfill_policy="multi_run")
    comp = comp.model_copy(update={"backfill_policy_max_partitions_per_run": 2})
    defs = comp.build_defs(None)
    multi_asset_def = next(a for a in defs.assets if hasattr(a, "node_def") and len(a.keys) > 1)
    assert multi_asset_def.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_unset_backfill_policy_leaves_dagsters_default():
    mod = load_component_module()
    comp = _single_sink_comp(mod, backfill_policy=None)
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy is None
