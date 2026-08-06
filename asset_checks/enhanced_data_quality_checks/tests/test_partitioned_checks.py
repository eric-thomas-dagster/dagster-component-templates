"""Tests for the v0.10.54 partitioned-asset-checks capability.

Covers the four precedence levels:
1. Absent config + partitioned asset → check inherits asset's partitions_def
2. `partitions_def: false` in check YAML → force unpartitioned check
3. `partitions_def: {type: daily, ...}` in check YAML → explicit override
4. Unpartitioned asset + no config → unpartitioned check
"""
import importlib.util
import pathlib

import pytest
import dagster as dg


_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("edqc", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

EnhancedDataQualityChecks = _mod.EnhancedDataQualityChecks


def _make_component_with_sibling_pdef(asset_name: str, partitions_def):
    """Instantiate the component + prime `_key_to_partitions_def` as if
    `_discover_sibling_assets` had run against a project where `asset_name`
    is a partitioned sibling asset."""
    c = EnhancedDataQualityChecks.__new__(EnhancedDataQualityChecks)
    c._key_to_partitions_def = {asset_name: partitions_def} if partitions_def else {}
    return c


def test_absent_config_inherits_partitions_def_from_asset():
    """Level 1: check config has no partitions_def → inherit from the target asset."""
    pdef = dg.DailyPartitionsDefinition(start_date="2025-01-01")
    c = _make_component_with_sibling_pdef("orders", pdef)
    resolved = c._resolve_check_partitions_def("orders", None)
    assert resolved is pdef


def test_explicit_false_forces_unpartitioned():
    """Level 2: `partitions_def: false` forces unpartitioned even if asset is partitioned."""
    pdef = dg.DailyPartitionsDefinition(start_date="2025-01-01")
    c = _make_component_with_sibling_pdef("orders", pdef)
    resolved = c._resolve_check_partitions_def("orders", False)
    assert resolved is None


def test_explicit_dict_override():
    """Level 3: `partitions_def: {type: weekly}` overrides asset's daily partitioning."""
    pdef_daily = dg.DailyPartitionsDefinition(start_date="2025-01-01")
    c = _make_component_with_sibling_pdef("orders", pdef_daily)
    resolved = c._resolve_check_partitions_def(
        "orders",
        {"type": "weekly", "start_date": "2025-01-01"},
    )
    assert isinstance(resolved, dg.WeeklyPartitionsDefinition)


def test_unpartitioned_asset_no_config():
    """Level 4: unpartitioned asset + no config → None."""
    c = _make_component_with_sibling_pdef("orders", None)
    resolved = c._resolve_check_partitions_def("orders", None)
    assert resolved is None


def test_invalid_partitions_def_shape_raises():
    """User error — dict missing 'type' key gets a clear message."""
    c = _make_component_with_sibling_pdef("orders", None)
    with pytest.raises(ValueError, match="invalid shape"):
        c._resolve_check_partitions_def("orders", {"start_date": "2025-01-01"})


def test_absent_asset_returns_none_gracefully():
    """Asset not in sibling discovery → returns None, no crash."""
    c = _make_component_with_sibling_pdef("orders", None)
    resolved = c._resolve_check_partitions_def("mystery_asset", None)
    assert resolved is None


@pytest.mark.parametrize("ptype,cls,start", [
    ("daily",   dg.DailyPartitionsDefinition,   "2025-01-01"),
    ("hourly",  dg.HourlyPartitionsDefinition,  "2025-01-01-00:00"),
    ("weekly",  dg.WeeklyPartitionsDefinition,  "2025-01-01"),
    ("monthly", dg.MonthlyPartitionsDefinition, "2025-01-01"),
])
def test_partitions_def_from_check_meta_time_types(ptype, cls, start):
    """The inline helper resolves all six shape types."""
    result = EnhancedDataQualityChecks._partitions_def_from_check_meta(
        {"type": ptype, "start_date": start}
    )
    assert isinstance(result, cls)


def test_partitions_def_from_check_meta_static():
    result = EnhancedDataQualityChecks._partitions_def_from_check_meta(
        {"type": "static", "values": ["US", "CA", "MX"]}
    )
    assert isinstance(result, dg.StaticPartitionsDefinition)


def test_partitions_def_from_check_meta_dynamic():
    result = EnhancedDataQualityChecks._partitions_def_from_check_meta(
        {"type": "dynamic", "name": "files"}
    )
    assert isinstance(result, dg.DynamicPartitionsDefinition)


def test_partitions_def_from_check_meta_missing_type_returns_none():
    """Absent `type` → None (not a partition)."""
    assert EnhancedDataQualityChecks._partitions_def_from_check_meta({}) is None
    assert EnhancedDataQualityChecks._partitions_def_from_check_meta(
        {"start_date": "2025-01-01"}
    ) is None
