"""snowpark_pipeline's backfill_policy was missing entirely (field + wiring), and its existing
partition_key computation only handled a single key (`context.partition_key if context.has_partition_key
else None`) -- a single_run backfill hands the asset a partition-key RANGE, which would have silently
produced partition_key=None instead of a real range representation (now a comma-joined range string).
This component pushes all compute server-side to a real Snowflake session, which this test suite has no
fixture for; build_defs() itself needs no connection, so this covers the field/wiring surface for real
without inventing a fake Snowpark session."""
from .conftest import load_component_module

import dagster as dg


def _comp(mod, backfill_policy=None):
    return mod.SnowparkPipelineComponent(
        asset_name="out_asset",
        connection={"account_env_var": "SF_ACCOUNT", "user_env_var": "SF_USER",
                    "password_env_var": "SF_PASSWORD", "warehouse": "WH", "database": "DB", "schema": "SCH"},
        source={"kind": "table", "table": "RAW.ORDERS"},
        operations=[{"op": "limit", "n": 10}],
        sink={"kind": "table", "table": "ANALYTICS.OUT", "mode": "overwrite"},
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )


def test_backfill_policy_single_run_sets_a_real_backfill_policy():
    mod = load_component_module()
    comp = _comp(mod, backfill_policy="single_run")
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_multi_run_caps_partitions_per_run():
    mod = load_component_module()
    comp = mod.SnowparkPipelineComponent(
        asset_name="out_asset",
        connection={"account_env_var": "SF_ACCOUNT", "user_env_var": "SF_USER",
                    "password_env_var": "SF_PASSWORD", "warehouse": "WH", "database": "DB", "schema": "SCH"},
        source={"kind": "table", "table": "RAW.ORDERS"},
        operations=[{"op": "limit", "n": 10}],
        sink={"kind": "table", "table": "ANALYTICS.OUT", "mode": "overwrite"},
        backfill_policy="multi_run",
        backfill_policy_max_partitions_per_run=3,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)


def test_unset_backfill_policy_leaves_dagsters_default():
    mod = load_component_module()
    comp = _comp(mod, backfill_policy=None)
    defs = comp.build_defs(None)
    asset = next(a for a in defs.assets if hasattr(a, "node_def"))
    assert asset.backfill_policy is None
