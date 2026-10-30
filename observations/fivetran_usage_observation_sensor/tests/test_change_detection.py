"""Real (DuckDB-backed, not mocked) tests for the core requirement: only emit
an AssetObservation when the summed MAR value for an asset has actually
changed since the last tick -- a daily-synced usage table polled hourly
should produce zero events on most ticks.

Also covers: the numeric metadata key Dagster+ Insights custom metrics would
register against (`fivetran/incremental_mar`), the default
schema_name/table_name -> AssetKey reconstruction matching dagster_fivetran's
own default translator, and demo_mode needing no real warehouse.
"""
import contextlib
from datetime import date, timedelta

import dagster as dg
import duckdb
import pytest

from .conftest import load_component_module

mod = load_component_module()
FivetranUsageObservationSensorComponent = mod.FivetranUsageObservationSensorComponent


class _FakeWarehouseResource:
    """`.get_connection()` context-manager shape, backed by a real DuckDB
    connection -- not a mock of a connection, an actual one."""

    def __init__(self, conn):
        self._conn = conn

    @contextlib.contextmanager
    def get_connection(self):
        yield self._conn


@pytest.fixture
def warehouse():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE incremental_mar ("
        "  schema_name VARCHAR, table_name VARCHAR, incremental_rows INTEGER,"
        "  measured_date DATE, free_type VARCHAR"
        ")"
    )
    yield conn
    conn.close()


def _insert(conn, schema_name, table_name, rows, days_ago=0, free_type="PAID"):
    conn.execute(
        "INSERT INTO incremental_mar VALUES (?, ?, ?, ?, ?)",
        [schema_name, table_name, rows, date.today() - timedelta(days=days_ago), free_type],
    )


def _component(**overrides):
    attrs = dict(sensor_name="fivetran_usage", resource_key="warehouse")
    attrs.update(overrides)
    return FivetranUsageObservationSensorComponent(**attrs)


def _run(comp, warehouse, cursor=None):
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context(resources={"warehouse": _FakeWarehouseResource(warehouse)}, cursor=cursor)
    return sensor_def(ctx)


def test_first_tick_emits_a_baseline_observation_per_asset(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component()
    result = _run(comp, warehouse)
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.asset_key == dg.AssetKey(["postgres_orders", "orders"])
    assert isinstance(event, dg.AssetObservation)
    assert event.metadata["fivetran/incremental_mar"].value == 1000


def test_unchanged_value_on_next_tick_emits_nothing(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component()
    result1 = _run(comp, warehouse)

    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert result2.asset_events == []
    assert "no changed MAR" in result2.skip_reason.skip_message


def test_changed_value_on_next_tick_emits_again_with_the_new_value(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component()
    result1 = _run(comp, warehouse)

    _insert(warehouse, "postgres_orders", "orders", 500, days_ago=1)  # restated prior day adds to the window's sum
    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].metadata["fivetran/incremental_mar"].value == 1500


def test_only_the_changed_asset_emits_not_the_unchanged_sibling(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    _insert(warehouse, "postgres_orders", "customers", 4200)
    comp = _component()
    result1 = _run(comp, warehouse)
    assert len(result1.asset_events) == 2

    _insert(warehouse, "postgres_orders", "orders", 300)  # only orders changes
    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].asset_key == dg.AssetKey(["postgres_orders", "orders"])


def test_free_type_filter_excludes_non_matching_rows_by_default(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000, free_type="FREE")
    comp = _component()
    result = _run(comp, warehouse)
    assert result.asset_events == []


def test_asset_key_prefix_is_applied_when_configured(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component(asset_key_prefix=["fivetran"])
    result = _run(comp, warehouse)
    assert result.asset_events[0].asset_key == dg.AssetKey(["fivetran", "postgres_orders", "orders"])


def test_emit_materialization_true_emits_materialization_instead_of_observation(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component(emit_materialization=True)
    result = _run(comp, warehouse)
    assert isinstance(result.asset_events[0], dg.AssetMaterialization)


def test_demo_mode_needs_no_real_warehouse():
    comp = FivetranUsageObservationSensorComponent(
        sensor_name="fivetran_usage_demo", resource_key="unused", demo_mode=True
    )
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))
    result1 = sensor_def(dg.build_sensor_context())
    assert len(result1.asset_events) == 3  # baseline: all three demo rows are "new"

    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    # The demo generator deliberately varies two of the three rows across ticks.
    assert 0 < len(result2.asset_events) < 3
