"""Real, committed tests for the one-asset-per-instance gap reported for
ExternalSqlAsset: `tables:` lets one component instance declare several
tables sharing one connection_string_env_var and (with
create_observation_sensor=true) one sensor/engine instead of one per table.
"""
import importlib.util
import pathlib
import types

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("esa_multi", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalSqlAsset = _mod.ExternalSqlAsset


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


def test_single_table_mode_still_works_unchanged():
    comp = ExternalSqlAsset(
        asset_key="warehouse/orders", table_name="orders", connection_string_env_var="DB_URL",
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 1
    assert dg.AssetKey(["warehouse", "orders"]) in specs


def test_tables_list_declares_several_assets_from_one_instance():
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        tables=[
            {"key": "warehouse/orders", "table_name": "orders"},
            {"key": "warehouse/customers", "table_name": "customers", "owners": ["team:data"]},
        ],
    )
    specs = _specs_by_key(comp.build_defs(None))
    assert len(specs) == 2
    assert specs[dg.AssetKey(["warehouse", "customers"])].owners == ["team:data"]


def test_both_shapes_set_is_rejected():
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        asset_key="warehouse/orders", table_name="orders",
        tables=[{"key": "warehouse/customers", "table_name": "customers"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_shape_set_is_rejected():
    comp = ExternalSqlAsset(connection_string_env_var="DB_URL")
    with pytest.raises(ValueError, match="Set either"):
        comp.build_defs(None)


def test_duplicate_keys_rejected():
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        tables=[
            {"key": "warehouse/orders", "table_name": "orders"},
            {"key": "warehouse/orders", "table_name": "orders_v2"},
        ],
    )
    with pytest.raises(ValueError, match="duplicate key"):
        comp.build_defs(None)


def test_multi_table_sensor_requires_explicit_sensor_name():
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        tables=[{"key": "warehouse/orders", "table_name": "orders"}],
        create_observation_sensor=True,
    )
    with pytest.raises(ValueError, match="sensor_name is required"):
        comp.build_defs(None)


class _FakeSqlClient:
    def __init__(self, data_versions):
        self.data_versions = data_versions
        self.calls = []

    def observe(self, table_name):
        self.calls.append(table_name)
        return {"data_version": self.data_versions[table_name]}


def test_shared_sensor_observes_every_table_over_one_resource_client():
    client = _FakeSqlClient({"orders": "10", "customers": "5"})
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        tables=[
            {"key": "warehouse/orders", "table_name": "orders"},
            {"key": "warehouse/customers", "table_name": "customers"},
        ],
        create_observation_sensor=True,
        sensor_name="sql_multi_sensor",
        resource_key="sql_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("sql_multi_sensor")
    context = dg.build_sensor_context(resources={"sql_client": client})
    result = sensor_def(context)

    assert client.calls == ["orders", "customers"]
    assert len(result.asset_events) == 2


def test_shared_sensor_asset_selection_covers_every_table():
    comp = ExternalSqlAsset(
        connection_string_env_var="DB_URL",
        tables=[
            {"key": "warehouse/orders", "table_name": "orders"},
            {"key": "warehouse/customers", "table_name": "customers"},
        ],
        create_observation_sensor=True,
        sensor_name="sql_multi_sensor",
        resource_key="sql_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("sql_multi_sensor")
    selected = set(sensor_def.asset_selection.selected_keys)
    assert selected == {dg.AssetKey(["warehouse", "orders"]), dg.AssetKey(["warehouse", "customers"])}
