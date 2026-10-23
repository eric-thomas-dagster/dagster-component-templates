"""Real, committed tests for GraphFirstAssetsComponent.

New component (not a bug fix): a "graph-first" demo mode, where the story
is lineage/checks/freshness/automation, not real data, needs a way to
declare N assets from YAML with a materialization body that's a genuine
no-op (component-feedback/2026-08-28-graph-first-assets.md).

Real dg.materialize() throughout, not just structural AssetSpec checks --
including a real lineage-only (no function-input) dependency between two
entries, which caught a real design bug before shipping: giving every
entry's no-op function the SAME default name ("_noop") made Dagster's
op-graph dependency resolution fail with "op _noop does not have input
<upstream>", even though AssetSpec.deps is meant to need no function input
at all. Fixed by deriving a unique op name per entry from its asset key;
this test pins that down so it can't regress silently.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("graph_first_assets_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
GraphFirstAssetsComponent = mod.GraphFirstAssetsComponent


def _materialize(comp):
    defs = comp.build_defs(None)
    with dg.instance_for_test() as instance:
        result = dg.materialize(list(defs.assets), instance=instance)
    return result


def test_single_asset_materializes_as_a_real_no_op():
    comp = GraphFirstAssetsComponent(assets=[
        {"key": "warehouse/orders", "group_name": "warehouse", "kinds": ["snowflake"]},
    ])
    result = _materialize(comp)
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert len(mats) == 1
    assert mats[0].event_specific_data.materialization.asset_key == dg.AssetKey(["warehouse", "orders"])


def test_lineage_only_dep_between_two_entries_does_not_require_a_function_input():
    """This is the exact bug caught before shipping: both entries' no-op
    functions defaulted to the same name ("_noop"), and Dagster's op-graph
    dependency resolution failed with "op _noop does not have input
    warehouse_orders" -- even though AssetSpec.deps is meant to need no
    function input. Fixed via a unique op name derived from each entry's key."""
    comp = GraphFirstAssetsComponent(assets=[
        {"key": "warehouse/orders", "group_name": "warehouse"},
        {"key": "warehouse/customers", "group_name": "warehouse", "deps": ["warehouse/orders"]},
    ])
    result = _materialize(comp)
    assert result.success
    mats = {
        e.event_specific_data.materialization.asset_key
        for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"
    }
    assert mats == {dg.AssetKey(["warehouse", "orders"]), dg.AssetKey(["warehouse", "customers"])}


def test_real_lineage_edge_is_visible_in_the_asset_graph():
    comp = GraphFirstAssetsComponent(assets=[
        {"key": "a"}, {"key": "b", "deps": ["a"]},
    ])
    defs = comp.build_defs(None)
    repo_def = dg.Definitions(assets=list(defs.assets)).get_repository_def()
    node_b = repo_def.asset_graph.get(dg.AssetKey(["b"]))
    assert dg.AssetKey(["a"]) in node_b.parent_keys


def test_metadata_owners_tags_description_carry_through_to_the_real_spec():
    comp = GraphFirstAssetsComponent(assets=[
        {
            "key": "a", "description": "A test asset", "owners": ["team:data"],
            "tags": {"tier": "gold"}, "metadata": {"source": "manual"},
        },
    ])
    defs = comp.build_defs(None)
    spec = next(iter(next(iter(defs.assets)).specs))
    assert spec.description == "A test asset"
    assert spec.owners == ["team:data"]
    assert spec.tags.get("tier") == "gold"
    assert spec.metadata.get("source") == "manual"


def test_partitioned_entry_builds_a_real_partitions_def_and_materializes():
    comp = GraphFirstAssetsComponent(assets=[
        {"key": "a", "partition_type": "daily", "partition_start": "2024-01-01"},
    ])
    defs = comp.build_defs(None)
    spec = next(iter(next(iter(defs.assets)).specs))
    assert isinstance(spec.partitions_def, dg.DailyPartitionsDefinition)

    with dg.instance_for_test() as instance:
        result = dg.materialize(list(defs.assets), instance=instance, partition_key="2024-01-02")
    assert result.success


def test_automation_condition_eager_resolves_to_a_real_condition():
    comp = GraphFirstAssetsComponent(assets=[{"key": "a", "automation_condition": "eager"}])
    spec = next(iter(next(iter(comp.build_defs(None).assets)).specs))
    assert spec.automation_condition is not None


def test_freshness_max_lag_minutes_builds_a_real_freshness_policy():
    comp = GraphFirstAssetsComponent(assets=[{"key": "a", "freshness_max_lag_minutes": 60}])
    spec = next(iter(next(iter(comp.build_defs(None).assets)).specs))
    assert spec.freshness_policy is not None


def test_missing_key_is_rejected():
    comp = GraphFirstAssetsComponent(assets=[{"group_name": "warehouse"}])
    with pytest.raises(ValueError, match="missing required field 'key'"):
        comp.build_defs(None)


def test_duplicate_key_is_rejected():
    comp = GraphFirstAssetsComponent(assets=[{"key": "a"}, {"key": "a"}])
    with pytest.raises(ValueError, match="duplicate key"):
        comp.build_defs(None)


def test_invalid_automation_condition_is_rejected():
    comp = GraphFirstAssetsComponent(assets=[{"key": "a", "automation_condition": "whenever"}])
    with pytest.raises(ValueError, match="unsupported automation_condition"):
        comp.build_defs(None)
