"""MySQLCDCMonitorSensorComponent: mocks mysql-replication's BinLogStreamReader and
pymysql.connect (the only real external surfaces) so these tests exercise table-pattern
resolution, event classification, position tracking (binlog + GTID), the purged-position
failure path, and the max_events_per_tick cap in isolation from a real MySQL server."""
import json

import dagster as dg
import pymysql
import pytest
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.gtid import GtidSet
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    PartialUpdateRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


# ── Fake binlog events — real subclasses of the library's own event classes
# (so isinstance() checks in component.py behave identically to production),
# with __init__ overridden to skip the real packet-parsing machinery. ──────
class _FakeWriteRowsEvent(WriteRowsEvent):
    def __init__(self, table, values):
        self.table = table
        self._rows = [{"values": values}]

    @property
    def rows(self):
        return self._rows


class _FakeUpdateRowsEvent(UpdateRowsEvent):
    def __init__(self, table, before_values, after_values):
        self.table = table
        self._rows = [{"before_values": before_values, "after_values": after_values}]

    @property
    def rows(self):
        return self._rows


class _FakePartialUpdateRowsEvent(PartialUpdateRowsEvent):
    def __init__(self, table, before_values, after_values):
        self.table = table
        self._rows = [{"before_values": before_values, "after_values": after_values}]

    @property
    def rows(self):
        return self._rows


class _FakeDeleteRowsEvent(DeleteRowsEvent):
    def __init__(self, table, values):
        self.table = table
        self._rows = [{"values": values}]

    @property
    def rows(self):
        return self._rows


class _FakeGtidEvent(GtidEvent):
    def __init__(self, gtid_str):
        self._gtid = gtid_str

    @property
    def gtid(self):
        return self._gtid


class _FakeBinLogStreamReader:
    """`events` is a list of (event, new_log_file, new_log_pos) -- position
    advances as each event is consumed, mirroring the real reader updating
    log_file/log_pos on every packet (even ones later filtered out)."""

    last_kwargs = None

    def __init__(self, events=None, raise_on_iterate=None, **kwargs):
        _FakeBinLogStreamReader.last_kwargs = kwargs
        self.log_file = kwargs.get("log_file") or "mysql-bin.000001"
        self.log_pos = kwargs.get("log_pos") or 4
        self._events = events or []
        self._raise_on_iterate = raise_on_iterate
        self.closed = False

    def __iter__(self):
        if self._raise_on_iterate:
            raise self._raise_on_iterate
        for ev, new_file, new_pos in self._events:
            self.log_file = new_file
            self.log_pos = new_pos
            yield ev

    def close(self):
        self.closed = True


class _FakeCursor:
    def __init__(self, tables, binary_logs):
        self._tables = tables
        self._binary_logs = binary_logs
        self._last_result = []

    def execute(self, sql, params=None):
        upper = sql.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper:
            self._last_result = [(t,) for t in self._tables]
        elif "SHOW BINARY LOGS" in upper:
            self._last_result = list(self._binary_logs)
        else:
            self._last_result = []

    def fetchall(self):
        return self._last_result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConnection:
    def __init__(self, tables, binary_logs):
        self._tables = tables
        self._binary_logs = binary_logs

    def cursor(self):
        return _FakeCursor(self._tables, self._binary_logs)

    def close(self):
        pass


def _patch_mysql(monkeypatch, mod, tables=("orders", "order_items_2024", "customers"), binary_logs=(("mysql-bin.000001", 4),), events=None, raise_on_iterate=None):
    monkeypatch.setattr(pymysql, "connect", lambda **kw: _FakeConnection(tables, binary_logs))

    def _fake_reader(**kwargs):
        return _FakeBinLogStreamReader(events=events, raise_on_iterate=raise_on_iterate, **kwargs)

    import pymysqlreplication
    monkeypatch.setattr(pymysqlreplication, "BinLogStreamReader", _fake_reader)


def _component(mod, **overrides):
    attrs = dict(
        sensor_name="cdc_sensor", host="db.internal", username="repl_user",
        server_id=1001, database="sales", table_patterns=["orders", "order_items_*"],
        job_name="apply_cdc_job",
    )
    attrs.update(overrides)
    return mod.MySQLCDCMonitorSensorComponent(**attrs)


def _tick(component, instance, cursor=None):
    sensor = list(component.build_defs(None).sensors)[0]
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


def test_table_patterns_resolve_to_exact_names_via_information_schema(mod, instance, monkeypatch):
    _patch_mysql(monkeypatch, mod, tables=("orders", "order_items_2024", "customers"), events=[])
    _tick(_component(mod), instance)
    assert _FakeBinLogStreamReader.last_kwargs["only_tables"] == ["order_items_2024", "orders"]
    assert _FakeBinLogStreamReader.last_kwargs["only_schemas"] == ["sales"]


def test_no_tables_match_pattern_skips(mod, instance, monkeypatch):
    _patch_mysql(monkeypatch, mod, tables=("customers", "invoices"), events=[])
    result = _tick(_component(mod), instance)
    assert result.run_requests in (None, [])
    assert "no tables" in str(result.skip_reason.skip_message).lower()


def test_insert_update_delete_classification(mod, instance, monkeypatch):
    events = [
        (_FakeWriteRowsEvent("orders", {"id": 1, "total": 9.99}), "mysql-bin.000001", 100),
        (_FakeUpdateRowsEvent("orders", {"id": 1, "total": 9.99}, {"id": 1, "total": 19.99}), "mysql-bin.000001", 200),
        (_FakeDeleteRowsEvent("orders", {"id": 1, "total": 19.99}), "mysql-bin.000001", 300),
    ]
    _patch_mysql(monkeypatch, mod, events=events)
    result = _tick(_component(mod), instance)

    config = result.run_requests[0].run_config["ops"]["config"]["config"]
    ops = [e["_cdc_op"] for e in config["events"]]
    assert ops == ["insert", "update", "delete"]
    assert config["events"][0]["before"] is None
    assert config["events"][0]["after"] == {"id": 1, "total": 9.99}
    assert config["events"][1]["before"] == {"id": 1, "total": 9.99}
    assert config["events"][1]["after"] == {"id": 1, "total": 19.99}
    assert config["events"][2]["before"] == {"id": 1, "total": 19.99}
    assert config["events"][2]["after"] is None

    cursor_data = json.loads(result.cursor)
    assert cursor_data == {"log_file": "mysql-bin.000001", "log_pos": 300}


def test_partial_update_event_classified_as_update_and_included_in_only_events(mod, instance, monkeypatch):
    events = [
        (_FakePartialUpdateRowsEvent("orders", {"id": 1, "meta": {"a": 1}}, {"id": 1, "meta": {"a": 2}}), "mysql-bin.000001", 150),
    ]
    _patch_mysql(monkeypatch, mod, events=events)
    result = _tick(_component(mod), instance)

    config = result.run_requests[0].run_config["ops"]["config"]["config"]
    assert config["events"][0]["_cdc_op"] == "update"
    assert config["events"][0]["after"] == {"id": 1, "meta": {"a": 2}}
    assert PartialUpdateRowsEvent in _FakeBinLogStreamReader.last_kwargs["only_events"]


def test_stored_position_is_passed_with_resume_stream_true(mod, instance, monkeypatch):
    _patch_mysql(monkeypatch, mod, events=[])
    cursor = json.dumps({"log_file": "mysql-bin.000005", "log_pos": 777})
    _tick(_component(mod), instance, cursor=cursor)
    kw = _FakeBinLogStreamReader.last_kwargs
    assert kw["log_file"] == "mysql-bin.000005"
    assert kw["log_pos"] == 777
    assert kw["resume_stream"] is True
    assert kw["blocking"] is False


def test_start_now_first_run_leaves_log_file_and_log_pos_unset(mod, instance, monkeypatch):
    _patch_mysql(monkeypatch, mod, events=[])
    _tick(_component(mod, on_first_run="start_now"), instance, cursor=None)
    kw = _FakeBinLogStreamReader.last_kwargs
    assert "log_file" not in kw
    assert "log_pos" not in kw
    # Still always resume_stream=True -- required even when the reader resolves its
    # own current position internally, otherwise it dumps from offset 4 regardless.
    assert kw["resume_stream"] is True


def test_from_beginning_first_run_uses_earliest_log_and_position_4(mod, instance, monkeypatch):
    _patch_mysql(monkeypatch, mod, binary_logs=(("mysql-bin.000001", 4), ("mysql-bin.000002", 4)), events=[])
    _tick(_component(mod, on_first_run="from_beginning"), instance, cursor=None)
    kw = _FakeBinLogStreamReader.last_kwargs
    assert kw["log_file"] == "mysql-bin.000001"
    assert kw["log_pos"] == 4


def test_purged_position_fails_loudly_and_does_not_advance_cursor(mod, instance, monkeypatch):
    err = pymysql.err.OperationalError(1236, "Could not find first log file name in binary log index file")
    _patch_mysql(monkeypatch, mod, raise_on_iterate=err)
    stored_cursor = json.dumps({"log_file": "mysql-bin.000001", "log_pos": 123})
    result = _tick(_component(mod), instance, cursor=stored_cursor)

    assert result.run_requests in (None, [])
    assert result.cursor is None  # never silently advances past a gap
    reason = str(result.skip_reason.skip_message).lower()
    assert "purged" in reason and "re-snapshot" in reason


def test_non_purge_operational_error_does_not_claim_purged(mod, instance, monkeypatch):
    err = pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
    _patch_mysql(monkeypatch, mod, raise_on_iterate=err)
    result = _tick(_component(mod), instance)
    reason = str(result.skip_reason.skip_message).lower()
    assert "purged" not in reason


def test_gtid_mode_merges_and_advances_stored_position(mod, instance, monkeypatch):
    uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
    events = [
        (_FakeGtidEvent(f"{uuid}:6"), "mysql-bin.000001", 500),
        (_FakeWriteRowsEvent("orders", {"id": 2}), "mysql-bin.000001", 600),
    ]
    _patch_mysql(monkeypatch, mod, events=events)
    stored_cursor = json.dumps({"gtid": f"{uuid}:1-5"})
    result = _tick(_component(mod, position_mode="gtid"), instance, cursor=stored_cursor)

    new_cursor = json.loads(result.cursor)
    merged = GtidSet(new_cursor["gtid"])
    assert str(merged) == f"{uuid}:1-6"
    assert _FakeBinLogStreamReader.last_kwargs["auto_position"] == f"{uuid}:1-5"


def test_gtid_quiet_tick_leaves_stored_position_unchanged(mod, instance, monkeypatch):
    uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
    _patch_mysql(monkeypatch, mod, events=[])
    stored_cursor = json.dumps({"gtid": f"{uuid}:1-5"})
    result = _tick(_component(mod, position_mode="gtid"), instance, cursor=stored_cursor)
    assert json.loads(result.cursor)["gtid"] == f"{uuid}:1-5"


def test_max_events_per_tick_caps_and_persists_partial_progress(mod, instance, monkeypatch):
    events = [
        (_FakeWriteRowsEvent("orders", {"id": i}), "mysql-bin.000001", 100 + i)
        for i in range(5)
    ]
    _patch_mysql(monkeypatch, mod, events=events)
    result = _tick(_component(mod, max_events_per_tick=2), instance)

    config = result.run_requests[0].run_config["ops"]["config"]["config"]
    assert config["event_count"] == 2
    cursor_data = json.loads(result.cursor)
    assert cursor_data["log_pos"] == 101  # position through the 2nd event only, not all 5


def test_quiet_tick_with_no_changes_still_persists_advanced_binlog_position(mod, instance, monkeypatch):
    # A tick can read filtered-out events (different table/schema) that still
    # advance the reader's position even though nothing reaches `events`.
    _patch_mysql(monkeypatch, mod, events=[])
    result = _tick(_component(mod), instance, cursor=json.dumps({"log_file": "mysql-bin.000001", "log_pos": 50}))
    assert result.run_requests in (None, [])
    assert result.cursor is not None


@pytest.mark.parametrize("field,value", [("position_mode", "bogus"), ("on_first_run", "bogus")])
def test_invalid_enum_fields_raise_at_build_time(mod, field, value):
    component = _component(mod, **{field: value})
    with pytest.raises(ValueError):
        component.build_defs(None)


def test_empty_table_patterns_raises_at_build_time(mod):
    component = _component(mod, table_patterns=[])
    with pytest.raises(ValueError):
        component.build_defs(None)


def test_jsonable_row_coerces_non_json_native_types(mod):
    from datetime import date, datetime
    from decimal import Decimal
    row = {
        "ts": datetime(2026, 1, 2, 3, 4, 5),
        "d": date(2026, 1, 2),
        "amount": Decimal("19.99"),
        "blob": b"hello",
        "n": 3,
        "s": "x",
        "flag": True,
        "none": None,
    }
    out = mod._jsonable_row(row)
    assert out["ts"] == "2026-01-02T03:04:05"
    assert out["d"] == "2026-01-02"
    assert out["amount"] == "19.99"
    assert out["blob"] == "hello"
    assert out["n"] == 3 and out["s"] == "x" and out["flag"] is True and out["none"] is None
    json.dumps(out)  # must actually be JSON-serializable
