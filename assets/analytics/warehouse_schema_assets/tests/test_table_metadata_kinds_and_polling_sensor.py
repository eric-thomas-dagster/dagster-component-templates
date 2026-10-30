"""Real, committed tests for three gaps found in review feedback:

1. `dg.TableMetadataSet` doesn't exist on the installed dagster package's
   top-level namespace (confirmed: hasattr(dagster, 'TableMetadataSet') is
   False) -- every table with introspected columns raised AttributeError.
   Fixed via the real, private import location.
2. `kinds` was hardcoded to {"sql"} with no way to override it.
3. No polling sensor -- changes to a table Dagster doesn't own were never
   observed. Added, opt-in, change-only-on-delta (same discipline as this
   registry's other polling sensors).
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
WarehouseSchemaAssetsComponent = mod.WarehouseSchemaAssetsComponent

_TABLES = [
    {
        "schema": "public",
        "table": "orders",
        "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
    },
]


def _component(**overrides):
    attrs = dict(database_url_env_var="WAREHOUSE_URL")
    attrs.update(overrides)
    return WarehouseSchemaAssetsComponent(**attrs)


def test_table_with_columns_no_longer_raises_attribute_error():
    """The original bug: AttributeError: module 'dagster' has no attribute
    'TableMetadataSet', for any table with include_columns=True (default)
    and at least one introspected column."""
    defs = mod._build_schema_defs(
        tables=_TABLES, key_prefix=None, group_by_schema=True, group_name=None, tag_with_schema=False,
    )
    spec = next(iter(defs.resolve_all_asset_specs()))
    assert spec.key == dg.AssetKey(["public", "orders"])
    assert spec.metadata.get("warehouse/column_count").value == 1


def test_kinds_defaults_to_sql():
    defs = mod._build_schema_defs(
        tables=_TABLES, key_prefix=None, group_by_schema=True, group_name=None, tag_with_schema=False,
    )
    spec = next(iter(defs.resolve_all_asset_specs()))
    assert spec.kinds == {"sql"}


def test_kinds_field_overrides_the_default():
    defs = mod._build_schema_defs(
        tables=_TABLES, key_prefix=None, group_by_schema=True, group_name=None, tag_with_schema=False,
        kinds=["postgres", "warehouse"],
    )
    spec = next(iter(defs.resolve_all_asset_specs()))
    assert spec.kinds == {"postgres", "warehouse"}


def test_polling_sensor_off_by_default(tmp_path):
    comp = _component()
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(_TABLES))
    defs = comp.build_defs_from_state(None, state_path)
    assert not defs.sensors


class _FakeResult:
    def __init__(self, count):
        self._count = count

    def fetchone(self):
        return (self._count,)


class _FakeConn:
    def __init__(self, count):
        self.count = count

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt):
        return _FakeResult(self.count)


class _FakeEngine:
    def __init__(self, count):
        self.count = count

    def connect(self):
        return _FakeConn(self.count)

    def dispose(self):
        pass


def test_polling_sensor_emits_only_on_row_count_change(tmp_path, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_URL", "postgresql://fake")
    comp = _component(polling_sensor=True)
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(_TABLES))
    defs = comp.build_defs_from_state(None, state_path)
    sensor_def = next(iter(defs.sensors))

    import sqlalchemy
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url: _FakeEngine(10))
    result1 = sensor_def(dg.build_sensor_context())
    assert result1.asset_events == []  # first sighting -- baseline only

    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url: _FakeEngine(10))
    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    assert result2.asset_events == []  # unchanged

    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url: _FakeEngine(15))
    result3 = sensor_def(dg.build_sensor_context(cursor=result2.cursor))
    assert len(result3.asset_events) == 1
    assert result3.asset_events[0].asset_key == dg.AssetKey(["public", "orders"])
    assert result3.asset_events[0].metadata["warehouse/row_count"].value == 15
