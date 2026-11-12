"""Committed tests for TrinoQueryComponent's resource_key retrofit.

No tests existed for this component before this retrofit. The `trino` client package is the
external boundary and is not installed in this environment, so `trino.dbapi.connect` /
`trino.auth.BasicAuthentication` are faked via `sys.modules` -- the component's lazy
`import trino.dbapi` / `from trino.auth import BasicAuthentication` pick up the fakes, and the
inline (no resource_key) path runs for real under dg.materialize() with a recording fake
connection. The resource_key path is exercised against a FakeTrinoResource mirroring
resources/trino_resource.TrinoResource's get_connection() shape, so it needs no `trino` import
at all -- confirming the component only ever touches the resource through that one method.
"""
import sys
import types
from contextlib import contextmanager

import dagster as dg
import pytest

from .conftest import load_component_module


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = [("id",), ("v",)]

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def fetchall(self):
        return [(1, "a"), (2, "b"), (3, "c")]


class FakeConn:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.close_calls += 1


class FakeTrinoResource:
    """Same get_connection() shape as resources/trino_resource.TrinoResource."""

    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def get_connection(self):
        yield self._conn


@pytest.fixture()
def fake_trino(monkeypatch):
    """Install a fake trino.dbapi/trino.auth module tree so the inline (no resource_key)
    path runs without the real `trino` package installed."""
    conns = []

    def fake_connect(**kwargs):
        c = FakeConn(**kwargs)
        conns.append(c)
        return c

    trino_mod = types.ModuleType("trino")
    dbapi_mod = types.ModuleType("trino.dbapi")
    dbapi_mod.connect = fake_connect
    auth_mod = types.ModuleType("trino.auth")

    class BasicAuthentication:
        def __init__(self, user, password):
            self.user, self.password = user, password

    auth_mod.BasicAuthentication = BasicAuthentication
    trino_mod.dbapi = dbapi_mod
    trino_mod.auth = auth_mod
    monkeypatch.setitem(sys.modules, "trino", trino_mod)
    monkeypatch.setitem(sys.modules, "trino.dbapi", dbapi_mod)
    monkeypatch.setitem(sys.modules, "trino.auth", auth_mod)

    class H:
        pass

    h = H()
    h.conns = conns
    return h


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    kw.setdefault("asset_name", "trino_out")
    kw.setdefault("catalog", "iceberg")
    kw.setdefault("query", "SELECT id, v FROM t")
    return mod.TrinoQueryComponent(**kw)


def test_resource_key_default_none_uses_inline_trino_dbapi(mod, fake_trino):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([asset_def])
    assert result.success
    df = result.output_for_node("trino_out")
    assert sorted(df["id"].tolist()) == [1, 2, 3]
    (conn,) = fake_trino.conns
    assert conn.close_calls == 1


def test_resource_key_set_uses_registered_resource_connection(mod):
    conn = FakeConn()
    res = FakeTrinoResource(conn)
    asset_def = _comp(mod, resource_key="trino_shared").build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"trino_shared"}
    result = dg.materialize([asset_def], resources={"trino_shared": res})
    assert result.success
    df = result.output_for_node("trino_out")
    assert sorted(df["id"].tolist()) == [1, 2, 3]
    assert conn.executed == ["SELECT id, v FROM t"]


def test_resource_key_not_registered_fails_loudly(mod):
    asset_def = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset_def])
