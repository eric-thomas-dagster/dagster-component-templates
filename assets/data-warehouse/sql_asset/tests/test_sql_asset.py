"""Committed tests for SqlAssetComponent.

Exercises both connection-acquisition shapes real resources in this repo use:
- a raw DBAPI connection returned directly (postgres_resource/mysql_resource/… shape)
- a @contextmanager-wrapped connection (trino_resource/starburst_resource shape)
"""
from contextlib import contextmanager

import dagster as dg
import pytest

from .conftest import load_component_module


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 3

    def execute(self, sql, parameters=None):
        self.conn.executed.append({"sql": sql, "parameters": parameters})

    def close(self):
        self.conn.cursor_closed = True


class FakeConn:
    def __init__(self, commit_raises=False):
        self.executed = []
        self.commit_calls = 0
        self.close_calls = 0
        self.cursor_closed = False
        self._commit_raises = commit_raises

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1
        if self._commit_raises:
            raise NotImplementedError("this driver has no transactions")

    def close(self):
        self.close_calls += 1


class RawConnectionResource:
    """Mimics postgres_resource/mysql_resource/…: get_connection() returns a raw connection."""

    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        return self._conn


class ContextManagerResource:
    """Mimics trino_resource/starburst_resource: get_connection() is itself a context manager."""

    def __init__(self, conn):
        self._conn = conn
        self.cm_exit_closed = False

    def get_connection(self):
        @contextmanager
        def _cm():
            try:
                yield self._conn
            finally:
                self._conn.close()
                self.cm_exit_closed = True
        return _cm()


class NoConnectionResource:
    pass


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    kw.setdefault("asset_key", "warehouse/transforms/rollup")
    kw.setdefault("resource_key", "db")
    return mod.SqlAssetComponent(**kw)


def test_requires_exactly_one_sql_source(mod):
    with pytest.raises(Exception, match="exactly one"):
        _comp(mod)
    with pytest.raises(Exception, match="exactly one"):
        _comp(mod, sql="SELECT 1", sql_statements=["SELECT 1"])


def test_resource_without_get_connection_fails_loudly(mod):
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    with pytest.raises(Exception, match="get_connection"):
        dg.materialize([asset_def], resources={"db": NoConnectionResource()})


def test_raw_connection_resource_runs_and_closes(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    assert conn.executed[0]["sql"] == "SELECT 1"
    assert conn.commit_calls == 1
    assert conn.close_calls == 1  # the ONLY close, since nullcontext does nothing on exit
    assert conn.cursor_closed


def test_context_manager_resource_runs_and_is_closed_by_both(mod):
    conn = FakeConn()
    res = ContextManagerResource(conn)
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": res})
    assert result.success
    assert conn.executed[0]["sql"] == "SELECT 1"
    assert conn.close_calls == 2  # closed once by the component, and again via the resource's own generator cleanup on exit
    assert res.cm_exit_closed


def test_sql_statements_list_runs_one_execute_per_statement(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql_statements=["DELETE FROM a", "INSERT INTO a VALUES (1)"]).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert [e["sql"] for e in conn.executed] == ["DELETE FROM a", "INSERT INTO a VALUES (1)"]


def test_sql_file_is_read_and_executed(mod, tmp_path):
    f = tmp_path / "q.sql"
    f.write_text("SELECT 1;")
    conn = FakeConn()
    asset_def = _comp(mod, sql_file=str(f)).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.executed[0]["sql"] == "SELECT 1;"


def test_parameters_passed_through_verbatim(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="DELETE FROM a WHERE d < %(cutoff)s", parameters={"cutoff": "2026-01-01"}).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.executed[0]["parameters"] == {"cutoff": "2026-01-01"}


def test_pre_sql_runs_before_main_sql(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1", pre_sql=["SET search_path TO analytics", "USE mydb"]).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert [e["sql"] for e in conn.executed] == ["SET search_path TO analytics", "USE mydb", "SELECT 1"]


def test_commit_failure_is_tolerated(mod):
    conn = FakeConn(commit_raises=True)
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    assert conn.commit_calls == 1
    assert conn.close_calls == 1  # cleanup still ran despite commit() raising
