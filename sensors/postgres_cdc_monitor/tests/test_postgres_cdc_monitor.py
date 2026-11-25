"""PostgresCDCMonitorSensorComponent: mocks psycopg2.connect and pgoutput.sql.read_changes
(the real external/network surfaces) so these tests exercise table-pattern resolution,
slot/publication auto-creation and sync, row-change classification (using the REAL
pgoutput Insert/Update/Delete/Relation/Converter -- not reimplemented), run_config shape,
and run-key dedup, in isolation from a real PostgreSQL server."""
import json
from unittest import mock

import dagster as dg
import pytest
from pgoutput import Column, Insert, Update, Delete, Relation, TupleData, TupleValue
from pgoutput.sql import Change

from .conftest import load_component_module

# Well-known Postgres system-catalog type OIDs, so the REAL Converter actually
# exercises its real type-parsing logic rather than needing a stub.
OID_INT4 = 23
OID_TEXT = 25


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


def _relation(name="orders", schema="public"):
    return Relation(
        relation_id=1,
        namespace=schema,
        name=name,
        replica_identity="d",
        columns=(
            Column(name="id", type_oid=OID_INT4, type_modifier=-1, is_key=True),
            Column(name="status", type_oid=OID_TEXT, type_modifier=-1, is_key=False),
        ),
    )


def _tuple(id_val: int, status_val: str) -> TupleData:
    return TupleData(values=(
        TupleValue(kind="t", data=str(id_val).encode()),
        TupleValue(kind="t", data=status_val.encode()),
    ))


class _FakeCursor:
    """Routes by substring on the executed SQL -- fakes pg_catalog.pg_tables,
    pg_replication_slots, pg_publication[_tables], and (via a monkeypatched
    read_changes, not through this cursor) nothing else real-SQL-shaped."""

    def __init__(self, tables, slot_exists_as=None, publication_tables=None):
        self.tables = tables
        self.slot_exists_as = slot_exists_as
        self.publication_tables = publication_tables  # None = publication doesn't exist
        self.executed = []
        self._last_result = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "pg_catalog.pg_tables" in sql:
            self._last_result = [(t,) for t in self.tables]
        elif "pg_replication_slots" in sql:
            self._last_result = [(self.slot_exists_as,)] if self.slot_exists_as else []
        elif "pg_create_logical_replication_slot" in sql:
            self._last_result = []
        elif "FROM pg_publication WHERE" in sql:
            self._last_result = [(1,)] if self.publication_tables is not None else []
        elif "CREATE PUBLICATION" in sql:
            self._last_result = []
        elif "pg_publication_tables" in sql:
            self._last_result = [(s, t) for s, t in (self.publication_tables or [])]
        elif "ALTER PUBLICATION" in sql:
            self._last_result = []
        else:
            self._last_result = []

    def fetchone(self):
        return self._last_result[0] if self._last_result else None

    def fetchall(self):
        return self._last_result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _patch_connect(monkeypatch, cursor):
    conn = _FakeConn(cursor)
    monkeypatch.setattr("psycopg2.connect", lambda **kw: conn)
    return conn


def _base_kwargs(**overrides):
    kwargs = dict(
        sensor_name="t", host="h", username="u", database="sales",
        slot_name="slot1", publication_name="pub1", table_patterns=["orders"],
        job_name="job1",
    )
    kwargs.update(overrides)
    return kwargs


def _run(mod, instance, sensor_def):
    ctx = dg.build_sensor_context(instance=instance)
    return sensor_def(ctx)


def test_resolves_tables_by_pattern_and_skips_when_none_match(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["widgets"], slot_exists_as="pgoutput", publication_tables=[])
    _patch_connect(monkeypatch, cursor)
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    result = _run(mod, instance, sensor_def)
    assert "No tables" in str(result.skip_reason.skip_message)


def test_creates_slot_and_publication_when_missing(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["orders"], slot_exists_as=None, publication_tables=None)
    _patch_connect(monkeypatch, cursor)
    monkeypatch.setattr(
        "pgoutput.sql.read_changes",
        lambda *a, **kw: [],
    )
    import sys
    monkeypatch.setitem(
        sys.modules, "pgoutput.sql",
        mock.Mock(read_changes=lambda *a, **kw: []),
    )
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    result = _run(mod, instance, sensor_def)
    executed_sql = " | ".join(sql for sql, _ in cursor.executed)
    assert "pg_create_logical_replication_slot" in executed_sql
    assert "CREATE PUBLICATION" in executed_sql
    assert str(result.skip_reason.skip_message) == "No new PostgreSQL CDC events found"


def test_rejects_slot_with_wrong_output_plugin(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["orders"], slot_exists_as="wal2json", publication_tables=[("public", "orders")])
    _patch_connect(monkeypatch, cursor)
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    result = _run(mod, instance, sensor_def)
    reason = str(result.skip_reason.skip_message)
    assert "wal2json" in reason
    assert "refusing" in reason


def test_syncs_publication_table_list(mod, instance, monkeypatch):
    # Publication currently tracks 'legacy_orders' (should be dropped) but not
    # 'orders' (should be added), since table_patterns only resolves 'orders'.
    cursor = _FakeCursor(
        tables=["orders", "legacy_orders_untracked"],
        slot_exists_as="pgoutput",
        publication_tables=[("public", "legacy_orders")],
    )
    _patch_connect(monkeypatch, cursor)
    import sys
    monkeypatch.setitem(sys.modules, "pgoutput.sql", mock.Mock(read_changes=lambda *a, **kw: []))
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs(table_patterns=["orders"]))
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    _run(mod, instance, sensor_def)
    executed_sql = " | ".join(sql for sql, _ in cursor.executed)
    assert "ADD TABLE" in executed_sql and '"public"."orders"' in executed_sql
    assert "DROP TABLE" in executed_sql and "legacy_orders" in executed_sql


def test_classifies_insert_update_delete_via_real_pgoutput(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["orders"], slot_exists_as="pgoutput", publication_tables=[("public", "orders")])
    _patch_connect(monkeypatch, cursor)

    rel = _relation()
    changes = [
        Change(lsn=100, xid=None, message=Insert(relation_id=1, new=_tuple(1, "new"), relation=rel)),
        Change(lsn=101, xid=None, message=Update(relation_id=1, new=_tuple(1, "shipped"), old=_tuple(1, "new"), old_kind="O", relation=rel)),
        Change(lsn=102, xid=None, message=Delete(relation_id=1, old=_tuple(1, "shipped"), old_kind="O", relation=rel)),
    ]

    calls = {"n": 0}

    def fake_read_changes(cur, slot, pubs, peek=True, upto_lsn=None, upto_nchanges=None):
        calls["n"] += 1
        if peek:
            return changes
        return []  # the consume call

    import sys
    monkeypatch.setitem(sys.modules, "pgoutput.sql", mock.Mock(read_changes=fake_read_changes))

    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    result = _run(mod, instance, sensor_def)

    assert len(result.run_requests) == 1
    cfg = result.run_requests[0].run_config["ops"]["config"]["config"]
    assert cfg["event_count"] == 3
    events = cfg["events"]
    assert events[0]["_cdc_op"] == "insert" and events[0]["before"] is None and events[0]["after"] == {"id": 1, "status": "new"}
    assert events[1]["_cdc_op"] == "update" and events[1]["before"] == {"id": 1, "status": "new"} and events[1]["after"] == {"id": 1, "status": "shipped"}
    assert events[2]["_cdc_op"] == "delete" and events[2]["after"] is None and events[2]["before"] == {"id": 1, "status": "shipped"}
    assert all(e["table"] == "public.orders" for e in events)
    # peek, then consume -- exactly 2 calls to read_changes.
    assert calls["n"] == 2


def test_run_key_is_deterministic_on_lsn_range(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["orders"], slot_exists_as="pgoutput", publication_tables=[("public", "orders")])
    _patch_connect(monkeypatch, cursor)
    rel = _relation()
    changes = [Change(lsn=50, xid=None, message=Insert(relation_id=1, new=_tuple(1, "x"), relation=rel))]

    import sys
    monkeypatch.setitem(
        sys.modules, "pgoutput.sql",
        mock.Mock(read_changes=lambda cur, slot, pubs, peek=True, upto_lsn=None, upto_nchanges=None: (changes if peek else [])),
    )
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    r1 = _run(mod, instance, sensor_def)
    r2 = _run(mod, instance, sensor_def)
    assert r1.run_requests[0].run_key == r2.run_requests[0].run_key == "t-50->50"


def test_missing_pgoutput_dependency_reports_skip_reason(mod, instance, monkeypatch):
    cursor = _FakeCursor(tables=["orders"], slot_exists_as="pgoutput", publication_tables=[("public", "orders")])
    _patch_connect(monkeypatch, cursor)
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "pgoutput" or name.startswith("pgoutput."):
            raise ImportError("no module named pgoutput")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs())
    defs = comp.build_defs(context=mock.Mock())
    sensor_def = list(defs.sensors)[0]
    result = _run(mod, instance, sensor_def)
    assert "pgoutput/psycopg2 is not installed" in str(result.skip_reason.skip_message)


def test_requires_at_least_one_table_pattern(mod):
    comp = mod.PostgresCDCMonitorSensorComponent.model_construct(**_base_kwargs(table_patterns=[]))
    with pytest.raises(ValueError, match="table_patterns"):
        comp.build_defs(context=mock.Mock())
