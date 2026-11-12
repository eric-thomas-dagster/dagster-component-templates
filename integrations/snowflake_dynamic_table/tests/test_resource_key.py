"""Committed tests for SnowflakeDynamicTableComponent's resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection for the inline-field path. The resource path uses a stub resource object whose
`.get_connection()` is a real contextmanager (mirroring dagster_snowflake.SnowflakeResource's
real API) so we assert it is actually called rather than the inline connector.
"""
import contextlib

import dagster as dg
import pytest
import snowflake.connector

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs=None):
        self.kwargs = kwargs or {}
        self.executed = []
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    return rec


class StubSnowflakeResource:
    """Stand-in for dagster_snowflake.SnowflakeResource: real get_connection() contextmanager API,
    but records the call and yields a fake connection instead of a real one."""

    def __init__(self):
        self.get_connection_calls = 0
        self.conn = FakeConn()

    @contextlib.contextmanager
    def get_connection(self):
        self.get_connection_calls += 1
        yield self.conn


def _comp(mod, **kw):
    kw.setdefault("asset_name", "dt_asset")
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    kw.setdefault("password", "pw")
    kw.setdefault("database", "DB")
    kw.setdefault("schema_name", "SC")
    kw.setdefault("warehouse", "WH")
    kw.setdefault("dt_name", "MY_DT")
    kw.setdefault("target_lag", "5 minutes")
    kw.setdefault("sql", "SELECT 1")
    return mod.SnowflakeDynamicTableComponent(**kw)


def test_resource_key_default_none_uses_inline_path(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([asset_def])
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "pw"
    assert conn.close_calls == 1
    assert len(conn.executed) == 1
    assert "CREATE OR REPLACE DYNAMIC TABLE DB.SC.MY_DT" in conn.executed[0]


def test_resource_key_set_uses_resource_get_connection_not_inline(mod, sf):
    asset_def = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    res = StubSnowflakeResource()
    result = dg.materialize([asset_def], resources={"snow": res})
    assert result.success
    assert res.get_connection_calls == 1
    assert res.conn.close_calls == 1
    assert len(res.conn.executed) == 1
    assert "CREATE OR REPLACE DYNAMIC TABLE DB.SC.MY_DT" in res.conn.executed[0]
    # inline connector path must NOT have been used
    assert sf.conns == []
