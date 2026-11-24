"""clickhouse_table_observation_sensor: `targets:` (N tables per instance) +
real cursor-carried skip-when-unchanged --
component-feedback/2026-11-23-aws-batch-pipes.md item 4.

test_resource_key.py's own `_run` helper builds a FRESH build_sensor_context
(cursor=None) every call, so it never exercises a real cursor hand-off
between ticks -- these tests pass an explicit cursor string from tick 1's
result into tick 2's context, the way Dagster's sensor daemon actually does
in production.
"""
import dagster as dg
import pytest
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import FakeClickHouseClient, FakeGetClientResource, FakeObserveResource, load_component_module

mod = load_component_module()
ClickHouseTableObservationSensorComponent = mod.ClickHouseTableObservationSensorComponent


def _run(comp, resources, cursor=None):
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context(resources=resources, cursor=cursor)
    return sensor_def(ctx)


# ── Mutual exclusivity ───────────────────────────────────────────────────────

def test_single_and_targets_mutually_exclusive():
    with pytest.raises(Exception, match="not both"):
        ClickHouseTableObservationSensorComponent(
            sensor_name="ch", asset_key="a", database="d", table="t",
            targets=[{"asset_key": "x", "database": "d2", "table": "t2"}],
        )


def test_requires_one_mode():
    with pytest.raises(Exception, match="Set either"):
        ClickHouseTableObservationSensorComponent(sensor_name="ch")


# ── targets: N tables, one sensor, one tick ─────────────────────────────────

def test_targets_declares_multiple_tables_one_sensor():
    comp = ClickHouseTableObservationSensorComponent(
        sensor_name="ch_multi",
        resource_key="ch_shared",
        targets=[
            {"asset_key": "clickhouse/analytics/events", "database": "analytics", "table": "events"},
            {"asset_key": "clickhouse/analytics/sessions", "database": "analytics", "table": "sessions"},
        ],
    )
    defs = comp.build_defs(None)
    assert len(list(defs.sensors)) == 1


def test_targets_each_queried_with_own_source():
    resource = FakeObserveResource({"data_version": "v1"})
    comp = ClickHouseTableObservationSensorComponent(
        sensor_name="ch_multi",
        resource_key="ch_shared",
        targets=[
            {"asset_key": "clickhouse/analytics/events", "database": "analytics", "table": "events"},
            {"asset_key": "clickhouse/analytics/sessions", "database": "analytics", "table": "sessions"},
        ],
    )
    _run(comp, {"ch_shared": resource})
    assert resource.calls == ["analytics.events", "analytics.sessions"]


def test_targets_emit_under_each_resolved_asset_key():
    resource = FakeObserveResource({"data_version": "v1"})
    comp = ClickHouseTableObservationSensorComponent(
        sensor_name="ch_multi",
        resource_key="ch_shared",
        targets=[
            {"asset_key": "clickhouse/analytics/events", "database": "analytics", "table": "events"},
            {"asset_key": "clickhouse/analytics/sessions", "database": "analytics", "table": "sessions"},
        ],
    )
    result = _run(comp, {"ch_shared": resource})
    keys = {e.asset_key for e in result.asset_events}
    assert keys == {
        dg.AssetKey.from_user_string("clickhouse/analytics/events"),
        dg.AssetKey.from_user_string("clickhouse/analytics/sessions"),
    }


def test_targets_with_get_client_only_resource_runs_system_table_queries_per_table():
    client = FakeClickHouseClient(row_count=7, engine="MergeTree")
    resource = FakeGetClientResource(client)
    comp = ClickHouseTableObservationSensorComponent(
        sensor_name="ch_multi",
        resource_key="ch_shared",
        targets=[
            {"asset_key": "clickhouse/a/t1", "database": "a", "table": "t1"},
            {"asset_key": "clickhouse/a/t2", "database": "a", "table": "t2"},
        ],
    )
    result = _run(comp, {"ch_shared": resource})
    assert len(client.commands) == 10  # 5 queries x 2 tables
    assert len(result.asset_events) == 2


# ── Real cursor hand-off: skip-when-unchanged (the core feedback fix) ──────

def _comp(**kw):
    args = dict(sensor_name="ch_obs", asset_key="clickhouse/analytics/events", database="analytics", table="events")
    args.update(kw)
    return ClickHouseTableObservationSensorComponent(**args)


def test_unchanged_data_version_emits_nothing_on_next_tick():
    comp = _comp(resource_key="ch_shared")
    tick1 = _run(comp, {"ch_shared": FakeObserveResource({"data_version": "5-2024-01-01"})})
    assert len(tick1.asset_events) == 1

    tick2 = _run(comp, {"ch_shared": FakeObserveResource({"data_version": "5-2024-01-01"})}, cursor=tick1.cursor)
    assert tick2.asset_events == []
    assert tick2.skip_reason.skip_message == "no target changed since last tick"


def test_changed_data_version_still_emits_on_next_tick():
    comp = _comp(resource_key="ch_shared")
    tick1 = _run(comp, {"ch_shared": FakeObserveResource({"data_version": "5-t1"})})
    tick2 = _run(comp, {"ch_shared": FakeObserveResource({"data_version": "6-t2"})}, cursor=tick1.cursor)
    assert len(tick2.asset_events) == 1
    assert tick2.asset_events[0].tags[DATA_VERSION_TAG] == "6-t2"


def test_native_path_also_skips_when_unchanged(monkeypatch):
    from .conftest import install_fake_clickhouse_connect
    monkeypatch.setenv("CH_HOST", "inline-host")
    client = FakeClickHouseClient(row_count=3, last_modified="2024-02-01", engine="MergeTree")
    install_fake_clickhouse_connect(monkeypatch, client)
    comp = _comp(host_env_var="CH_HOST")

    tick1 = _run(comp, {})
    assert len(tick1.asset_events) == 1

    tick2 = _run(comp, {}, cursor=tick1.cursor)
    assert tick2.asset_events == []


def test_targets_tracked_independently_in_cursor():
    """One table changing must not suppress -- or force -- emission for the other."""
    comp = ClickHouseTableObservationSensorComponent(
        sensor_name="ch_multi",
        resource_key="ch_shared",
        targets=[
            {"asset_key": "clickhouse/a/t1", "database": "a", "table": "t1"},
            {"asset_key": "clickhouse/a/t2", "database": "a", "table": "t2"},
        ],
    )

    class _MultiObserveResource:
        def __init__(self, versions):
            self._versions = versions

        def observe(self, source):
            return {"data_version": self._versions[source]}

    tick1 = _run(comp, {"ch_shared": _MultiObserveResource({"a.t1": "v1", "a.t2": "v1"})})
    assert len(tick1.asset_events) == 2

    tick2 = _run(comp, {"ch_shared": _MultiObserveResource({"a.t1": "v1", "a.t2": "v2"})}, cursor=tick1.cursor)
    assert len(tick2.asset_events) == 1
    assert tick2.asset_events[0].asset_key == dg.AssetKey.from_user_string("clickhouse/a/t2")
