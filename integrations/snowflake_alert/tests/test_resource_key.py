"""Committed tests for SnowflakeAlertComponent's resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection. The resource path uses a stub resource object exposing the real
dagster_snowflake.SnowflakeResource.get_connection() contract (a context manager yielding a
connection); the component runs for real under dg.materialize().
"""
import contextlib

import dagster as dg
import pytest
import snowflake.connector

from .conftest import load_component_module


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
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
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
    kw.setdefault("asset_name", "alert_asset")
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    kw.setdefault("password", "pw")
    kw.setdefault("database", "DB")
    kw.setdefault("schema_name", "SC")
    kw.setdefault("alert_name", "MY_ALERT")
    kw.setdefault("condition_sql", "SELECT 1")
    kw.setdefault("action_sql", "CALL do_thing()")
    kw.setdefault("warehouse", "WH")
    return mod.SnowflakeAlertComponent(**kw)


def test_resource_key_default_none_uses_inline_path(mod, sf):
    asset = _comp(mod).build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == set()
    result = dg.materialize([asset])
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "pw"
    assert conn.close_calls == 1
    assert conn.executed == [
        "CREATE OR REPLACE ALERT DB.SC.MY_ALERT\n  WAREHOUSE = WH\n  SCHEDULE = '60 minute'\n  IF (EXISTS (SELECT 1))\n  THEN CALL do_thing()",
        "ALTER ALERT DB.SC.MY_ALERT RESUME",
    ]


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
    assert fake_conn.executed[0].startswith("CREATE OR REPLACE ALERT DB.SC.MY_ALERT")


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset])


def test_resource_key_unset_still_requires_inline_auth(mod, sf):
    # Baseline inline-path behavior (unchanged by the retrofit): no password/authenticator
    # configured -> the component's own ValueError, not a resource-path error.
    asset = _comp(mod, password=None).build_defs(None).assets[0]
    with pytest.raises(ValueError, match="must set password OR authenticator"):
        dg.materialize([asset])
