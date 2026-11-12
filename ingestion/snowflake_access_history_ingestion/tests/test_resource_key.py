"""Committed tests for SnowflakeAccessHistoryIngestionComponent's resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection, and `pandas.read_sql` is faked to avoid needing a real DBAPI cursor protocol
(it just records the (sql, conn) it was called with and returns a small DataFrame). The
resource path uses a stub resource object exposing the real dagster_snowflake.SnowflakeResource
contract -- get_connection(), a context manager yielding a connection. The component runs for
real under dg.materialize().
"""
import contextlib

import dagster as dg
import pandas as pd
import pytest
import snowflake.connector

from .conftest import load_component_module


class FakeConn:
    def __init__(self, kwargs=None):
        self.kwargs = kwargs or {}
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []
        self.read_sql_calls = []  # (sql, conn)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    def fake_read_sql(sql, con, *a, **k):
        rec.read_sql_calls.append((sql, con))
        return pd.DataFrame({"a": [1, 2]})

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return rec


class FakeResource:
    """Stub standing in for a registered dagster_snowflake.SnowflakeResource.

    Only implements the one method the component calls: get_connection(), a
    context manager yielding a connection (the real contract).
    """

    def __init__(self, conn):
        self._conn = conn
        self.get_connection_calls = 0

    @contextlib.contextmanager
    def get_connection(self):
        self.get_connection_calls += 1
        yield self._conn


def _comp(mod, **kw):
    kw.setdefault("asset_name", "access_history")
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    return mod.SnowflakeAccessHistoryIngestionComponent(**kw)


def test_resource_key_default_none_uses_inline_path(mod, sf):
    asset = _comp(mod, warehouse="WH", role="ACCOUNTADMIN").build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([asset])
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "env_pw"
    assert conn.kwargs["warehouse"] == "WH"
    assert conn.kwargs["role"] == "ACCOUNTADMIN"
    assert conn.close_calls == 1
    # inline path really used the fake snowflake.connector connection for the query
    (sql, used_conn) = sf.read_sql_calls[0]
    assert used_conn is conn
    assert "ACCESS_HISTORY" in sql


def test_resource_key_set_uses_resource_get_connection_and_ignores_inline(mod, sf):
    asset = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == {"snow"}
    fake_conn = FakeConn()
    fake_resource = FakeResource(fake_conn)
    result = dg.materialize([asset], resources={"snow": fake_resource})
    assert result.success
    # inline snowflake.connector.connect must NOT have been used
    assert sf.conns == []
    assert fake_resource.get_connection_calls == 1
    assert fake_conn.close_calls == 1
    (sql, used_conn) = sf.read_sql_calls[0]
    assert used_conn is fake_conn


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset])
