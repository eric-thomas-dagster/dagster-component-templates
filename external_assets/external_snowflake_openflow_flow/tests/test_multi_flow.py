"""Real, committed tests for the same one-asset-per-instance gap reported for
ExternalKafkaAsset, found independently here: `flows:` lets one component
instance declare several OpenFlow process groups instead of one block per flow.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("esoff_multi", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalSnowflakeOpenflowFlowAsset = _mod.ExternalSnowflakeOpenflowFlowAsset


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


def test_single_flow_mode_still_works_unchanged():
    comp = ExternalSnowflakeOpenflowFlowAsset(
        asset_key="snowflake/openflow/customer_sync", flow_name="customer_sync",
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 1
    assert dg.AssetKey(["snowflake", "openflow", "customer_sync"]) in specs


def test_flows_list_declares_several_assets_from_one_instance():
    comp = ExternalSnowflakeOpenflowFlowAsset(
        flows=[
            {"key": "snowflake/openflow/customer_sync", "flow_name": "customer_sync"},
            {"key": "snowflake/openflow/order_sync", "flow_name": "order_sync", "runtime_id": "rt-1"},
        ],
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 2
    assert specs[dg.AssetKey(["snowflake", "openflow", "order_sync"])].metadata["openflow_runtime_id"] == "rt-1"


def test_both_shapes_set_is_rejected():
    comp = ExternalSnowflakeOpenflowFlowAsset(
        asset_key="snowflake/openflow/customer_sync", flow_name="customer_sync",
        flows=[{"key": "snowflake/openflow/order_sync", "flow_name": "order_sync"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_shape_set_is_rejected():
    comp = ExternalSnowflakeOpenflowFlowAsset()
    with pytest.raises(ValueError, match="Set either"):
        comp.build_defs(None)


def test_duplicate_keys_rejected():
    comp = ExternalSnowflakeOpenflowFlowAsset(
        flows=[
            {"key": "snowflake/openflow/customer_sync", "flow_name": "customer_sync"},
            {"key": "snowflake/openflow/customer_sync", "flow_name": "customer_sync_v2"},
        ],
    )
    with pytest.raises(ValueError, match="duplicate key"):
        comp.build_defs(None)
