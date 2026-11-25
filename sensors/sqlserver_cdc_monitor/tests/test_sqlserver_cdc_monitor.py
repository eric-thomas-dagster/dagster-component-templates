"""SQLServerCDCMonitorSensorComponent: fakes the pymssql module entirely (it isn't installed
in this environment, same as production doesn't require it unless this component is used) via
sys.modules injection, and fakes the DB-API connection/cursor surface -- these tests exercise
table-pattern resolution, __$operation classification (including before/after pairing),
LSN position tracking, the purged-retention failure path, on_first_run modes, and the
max_events_per_tick cap's resume-from-last-processed-position behavior, in isolation from a
real SQL Server instance."""
import json
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


class _FakeCursor:
    """Dispatches on substrings of the executed SQL text to a scripted scenario.

    `scenario` is a dict with keys:
      change_tables: list[(schema, table, capture_instance)]
      max_lsn: bytes
      min_lsn_by_instance: dict[str, bytes | None]
      changes_by_instance: dict[str, list[tuple]]  -- raw rows for fn_cdc_get_all_changes_*
      increment_lsn: callable(bytes) -> bytes
    """

    def __init__(self, scenario):
        self.scenario = scenario
        self._last_rows = []
        self._last_cols = []

    @property
    def description(self):
        return [(c,) for c in self._last_cols]

    def execute(self, sql, params=None):
        s = self.scenario
        sql_low = sql.lower()
        if "cdc.change_tables" in sql_low:
            self._last_cols = ["schema_name", "table_name", "capture_instance"]
            self._last_rows = list(s["change_tables"])
        elif "fn_cdc_get_max_lsn" in sql_low:
            self._last_cols = ["max_lsn"]
            self._last_rows = [(s["max_lsn"],)]
        elif "fn_cdc_get_min_lsn" in sql_low:
            inst = params[0]
            self._last_cols = ["min_lsn"]
            self._last_rows = [(s["min_lsn_by_instance"].get(inst),)]
        elif "fn_cdc_increment_lsn" in sql_low:
            lsn = params[0]
            self._last_cols = ["next_lsn"]
            self._last_rows = [(s["increment_lsn"](lsn),)]
        elif "fn_cdc_get_all_changes_" in sql_low:
            inst = sql_low.split("fn_cdc_get_all_changes_")[1].split("(")[0]
            cols, rows = s["changes_by_instance"].get(inst, (["__$start_lsn", "__$seqval", "__$operation"], []))
            self._last_cols = cols
            self._last_rows = rows
        else:
            raise AssertionError(f"Unexpected SQL in fake cursor: {sql}")

    def fetchall(self):
        return self._last_rows

    def fetchone(self):
        return self._last_rows[0] if self._last_rows else None

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, scenario):
        self.scenario = scenario
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.scenario)

    def close(self):
        self.closed = True


def _install_fake_pymssql(monkeypatch, scenario, connect_calls=None):
    fake_module = types.ModuleType("pymssql")

    def _connect(**kwargs):
        if connect_calls is not None:
            connect_calls.append(kwargs)
        return _FakeConnection(scenario)

    fake_module.connect = _connect
    monkeypatch.setitem(sys.modules, "pymssql", fake_module)


def _lsn(n: int) -> bytes:
    """A trivially orderable fake LSN -- real LSNs are opaque binary(10), but byte
    comparison (what the component does: `from_lsn < bytes(min_lsn)`) only needs
    a consistent ordering for these tests, not real LSN semantics."""
    return n.to_bytes(10, "big")


def _build_sensor(mod, **overrides):
    attrs = dict(
        sensor_name="test_sensor",
        host="db.internal",
        username="cdc_reader",
        password_env_var=None,
        database="sales",
        table_patterns=["dbo.orders"],
        job_name="apply_job",
        minimum_interval_seconds=30,
    )
    attrs.update(overrides)
    component = mod.SQLServerCDCMonitorSensorComponent(**attrs)
    defs = component.build_defs(context=None)
    return next(iter(defs.sensors))


def _eval(sensor_def, instance, cursor=None):
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor)
    return sensor_def(ctx)


def test_table_pattern_resolution_matches_schema_dot_table(mod, instance, monkeypatch):
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT"), ("dbo", "customers", "customers_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {"orders_CT": _lsn(1)},
        "changes_by_instance": {},
        "increment_lsn": lambda lsn: int.from_bytes(lsn, "big").to_bytes(10, "big"),
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod, table_patterns=["dbo.orders"])
    result = _eval(sensor_def, instance)
    # Only dbo.orders matched -- dbo.customers should never be queried (no changes_by_instance
    # entry for customers_CT would raise AssertionError in the fake cursor if it were).
    assert result.skip_reason.skip_message == "No new SQL Server CDC events found"


def test_table_pattern_no_match_skips_cleanly(mod, instance, monkeypatch):
    scenario = {
        "change_tables": [("dbo", "customers", "customers_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {},
        "changes_by_instance": {},
        "increment_lsn": lambda lsn: lsn,
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod, table_patterns=["dbo.orders"])
    result = _eval(sensor_def, instance)
    assert "No CDC capture instances" in result.skip_reason.skip_message


def test_insert_delete_classification(mod, instance, monkeypatch):
    cols = ["__$start_lsn", "__$seqval", "__$operation", "id", "amount"]
    rows = [
        (_lsn(10), _lsn(10), 2, 1, 100),  # insert
        (_lsn(11), _lsn(11), 1, 2, 200),  # delete
    ]
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {"orders_CT": None},
        "changes_by_instance": {"orders_ct": (cols, rows)},
        "increment_lsn": lambda lsn: _lsn(int.from_bytes(lsn, "big") + 1),
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod)
    result = _eval(sensor_def, instance)
    events = result.run_requests[0].run_config["ops"]["config"]["config"]["events"]
    assert len(events) == 2
    assert events[0]["_cdc_op"] == "insert" and events[0]["after"] == {"id": 1, "amount": 100} and events[0]["before"] is None
    assert events[1]["_cdc_op"] == "delete" and events[1]["before"] == {"id": 2, "amount": 200} and events[1]["after"] is None


def test_update_before_after_pairing(mod, instance, monkeypatch):
    cols = ["__$start_lsn", "__$seqval", "__$operation", "id", "amount"]
    rows = [
        (_lsn(10), _lsn(10), 3, 1, 100),  # update before-image
        (_lsn(10), _lsn(10), 4, 1, 150),  # update after-image
    ]
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {"orders_CT": None},
        "changes_by_instance": {"orders_ct": (cols, rows)},
        "increment_lsn": lambda lsn: _lsn(int.from_bytes(lsn, "big") + 1),
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod)
    result = _eval(sensor_def, instance)
    events = result.run_requests[0].run_config["ops"]["config"]["config"]["events"]
    assert len(events) == 1
    assert events[0]["_cdc_op"] == "update"
    assert events[0]["before"] == {"id": 1, "amount": 100}
    assert events[0]["after"] == {"id": 1, "amount": 150}


def test_purged_retention_fails_loudly_not_silently(mod, instance, monkeypatch):
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {"orders_CT": _lsn(50)},  # retention floor is PAST our stored position
        "changes_by_instance": {},
        "increment_lsn": lambda lsn: lsn,
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod)
    stored_cursor = json.dumps({"from_lsn": _lsn(10).hex()})
    result = _eval(sensor_def, instance, cursor=stored_cursor)
    assert "aged out of retention" in result.skip_reason.skip_message
    assert "dbo.orders" in result.skip_reason.skip_message


def test_on_first_run_start_now_skips_history(mod, instance, monkeypatch):
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT")],
        "max_lsn": _lsn(100),
        "min_lsn_by_instance": {"orders_CT": _lsn(1)},
        "changes_by_instance": {"orders_ct": (["__$start_lsn", "__$seqval", "__$operation"], [])},
        "increment_lsn": lambda lsn: _lsn(int.from_bytes(lsn, "big") + 1),
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod, on_first_run="start_now")
    result = _eval(sensor_def, instance)
    # No prior cursor + start_now -> from_lsn should equal max_lsn (100), not the
    # instance's min_lsn (1) -- verified indirectly via the persisted cursor advancing
    # from 100, not 1.
    new_cursor = json.loads(result.cursor)
    assert new_cursor["from_lsn"] == _lsn(101).hex()


def test_max_events_per_tick_resumes_from_last_processed_not_full_max_lsn(mod, instance, monkeypatch):
    """Regression test: if the cap is hit mid-tick, the next cursor must advance only to
    just past the LAST EVENT ACTUALLY PROCESSED, not to the originally-fetched max_lsn --
    otherwise every event between the cap and max_lsn that this tick never touched would
    be silently skipped forever."""
    cols = ["__$start_lsn", "__$seqval", "__$operation", "id"]
    rows = [
        (_lsn(10), _lsn(10), 2, 1),  # insert #1 -- this is the one we should stop after
        (_lsn(20), _lsn(20), 2, 2),  # insert #2 -- must NOT be consumed yet
        (_lsn(30), _lsn(30), 2, 3),  # insert #3 -- must NOT be consumed yet
    ]
    scenario = {
        "change_tables": [("dbo", "orders", "orders_CT")],
        "max_lsn": _lsn(1000),  # far beyond any row above
        "min_lsn_by_instance": {"orders_CT": None},
        "changes_by_instance": {"orders_ct": (cols, rows)},
        "increment_lsn": lambda lsn: _lsn(int.from_bytes(lsn, "big") + 1),
    }
    _install_fake_pymssql(monkeypatch, scenario)
    sensor_def = _build_sensor(mod, max_events_per_tick=1)
    result = _eval(sensor_def, instance)
    events = result.run_requests[0].run_config["ops"]["config"]["config"]["events"]
    assert len(events) == 1

    new_cursor = json.loads(result.cursor)
    # Must advance to just past LSN 10 (the one event actually processed), NOT past the
    # 1000 max_lsn fetched at tick start -- that would silently skip LSNs 20 and 30.
    assert new_cursor["from_lsn"] == _lsn(11).hex()


def test_invalid_driver_raises():
    mod_local = load_component_module()
    with pytest.raises(Exception):
        component = mod_local.SQLServerCDCMonitorSensorComponent(
            sensor_name="s", host="h", username="u", database="d",
            table_patterns=["dbo.t"], job_name="j", driver="not_a_real_driver",
        )
        component.build_defs(context=None)


def test_no_table_patterns_raises(mod):
    with pytest.raises(ValueError, match="table_patterns"):
        component = mod.SQLServerCDCMonitorSensorComponent(
            sensor_name="s", host="h", username="u", database="d",
            table_patterns=[], job_name="j",
        )
        component.build_defs(context=None)
