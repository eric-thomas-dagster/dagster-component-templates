"""Real (DuckDB-backed, not mocked) tests for the core requirements:

1. Only emit an AssetObservation when an asset's cumulative month-to-date MAR
   has actually changed since the last tick -- a daily-synced usage table
   polled hourly should produce zero events on most ticks.
2. A restatement to an EARLIER day this month -- including one old enough
   that a short rolling lookback window would have missed it entirely -- is
   still picked up, because the whole month is recomputed every tick rather
   than a trailing N-day window.
3. The reported numeric metric is a DELTA (net change since the last tick),
   not the cumulative level -- Dagster+ Insights custom metrics have no
   configurable aggregation and sum whatever's reported under a key, so
   reporting the level itself would make the Insights chart balloon with
   every tick instead of reflecting the real total.
4. The per-asset running total resets at a calendar-month boundary, so the
   first tick of a new month reports that month's fresh total as its delta,
   not a large negative blip from diffing against last month's final number.

Also covers: the default schema_name/table_name -> AssetKey reconstruction
matching dagster_fivetran's own default translator, and demo_mode needing no
real warehouse.

Dates are built from `date.today().replace(day=1)` (the query's own
month-start) rather than raw `days_ago` subtraction, so these tests can't
flake near a real month boundary.
"""
import contextlib
import json
from datetime import date

import dagster as dg
import duckdb
import pytest

from .conftest import load_component_module

mod = load_component_module()
FivetranUsageObservationSensorComponent = mod.FivetranUsageObservationSensorComponent

_MONTH_START = date.today().replace(day=1)
_TODAY = date.today()


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


def _insert(conn, schema_name, table_name, rows, on=_TODAY, free_type="PAID"):
    conn.execute(
        "INSERT INTO incremental_mar VALUES (?, ?, ?, ?, ?)",
        [schema_name, table_name, rows, on, free_type],
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


def test_first_tick_reports_the_full_total_as_its_delta(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component()
    result = _run(comp, warehouse)
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.asset_key == dg.AssetKey(["postgres_orders", "orders"])
    assert isinstance(event, dg.AssetObservation)
    assert event.metadata["fivetran/mar"].value == 1000
    assert event.metadata["fivetran/mar_month_to_date"].value == "1000"


def test_unchanged_value_on_next_tick_emits_nothing(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000)
    comp = _component()
    result1 = _run(comp, warehouse)

    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert result2.asset_events == []
    assert "no changed MAR" in result2.skip_reason.skip_message


def test_new_data_today_reports_only_the_increment_not_the_new_total(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 1000, on=_TODAY)
    comp = _component()
    result1 = _run(comp, warehouse)
    assert result1.asset_events[0].metadata["fivetran/mar"].value == 1000

    _insert(warehouse, "postgres_orders", "orders", 500, on=_TODAY)
    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert len(result2.asset_events) == 1
    # The cumulative total is now 1500, but the DELTA reported (meant to be
    # summed by Insights) must be just the 500 that's new -- summing 1000 and
    # 500 reconstructs the real 1500 total; summing 1000 and 1500 would not.
    assert result2.asset_events[0].metadata["fivetran/mar"].value == 500
    assert result2.asset_events[0].metadata["fivetran/mar_month_to_date"].value == "1500"


def test_restatement_of_an_earlier_day_this_month_is_still_picked_up(warehouse):
    """The exact scenario a short rolling lookback window would get wrong:
    a correction to a day old enough to have already fallen out of a
    trailing N-day window. Recomputing the full month every tick means
    there's no window for it to fall outside of."""
    _insert(warehouse, "postgres_orders", "orders", 1000, on=_MONTH_START)
    comp = _component()
    result1 = _run(comp, warehouse)
    assert result1.asset_events[0].metadata["fivetran/mar"].value == 1000

    # Fivetran restates month_start's row upward, well after the fact --
    # a lookback_days=2-style window would already have moved past this date.
    _insert(warehouse, "postgres_orders", "orders", 400, on=_MONTH_START)
    result2 = _run(comp, warehouse, cursor=result1.cursor)
    assert len(result2.asset_events) == 1
    # Delta of the restatement itself, not the new 1400 total.
    assert result2.asset_events[0].metadata["fivetran/mar"].value == 400


def test_new_month_resets_the_baseline_instead_of_reporting_a_negative_blip(warehouse):
    _insert(warehouse, "postgres_orders", "orders", 500, on=_TODAY)
    comp = _component()

    # Simulate a cursor left over from a prior month with a much larger
    # running total -- without a reset, naively diffing (10 - 5000) would
    # report a nonsensical large negative delta for the start of a new month.
    stale_cursor = json.dumps({
        "tick": 50,
        "month": "2020-01",
        "last_mar": {"postgres_orders/orders": 5000.0},
    })
    result = _run(comp, warehouse, cursor=stale_cursor)
    assert len(result.asset_events) == 1
    assert result.asset_events[0].metadata["fivetran/mar"].value == 500


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
    assert len(result1.asset_events) == 3  # baseline: all three demo rows report their full total as the delta

    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    # The demo generator deliberately grows two of the three rows across ticks.
    assert 0 < len(result2.asset_events) < 3
