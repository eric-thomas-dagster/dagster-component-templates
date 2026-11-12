"""Committed tests for SnowflakeTaskComponent: resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection. The resource path uses the REAL dagster_snowflake.SnowflakeResource.get_connection();
the component runs for real under dg.materialize().
"""
import dagster as dg
import pytest
import snowflake.connector
from dagster_snowflake import SnowflakeResource

from .conftest import load_component_module


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

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


def _comp(mod, **kw):
    kw.setdefault("asset_name", "task_asset")
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    kw.setdefault("password", "pw")
    kw.setdefault("database", "DB")
    kw.setdefault("schema_name", "SC")
    kw.setdefault("task_name", "MY_TASK")
    kw.setdefault("sql", "CALL my_proc()")
    kw.setdefault("on_materialize", "create_only")
    return mod.SnowflakeTaskComponent(**kw)


def test_resource_key_default_none_uses_inline_fields_and_closes_connection(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "pw"
    assert conn.close_calls == 1
    assert any("CREATE OR REPLACE TASK" in e for e in conn.executed)


def test_resource_key_set_uses_real_snowflake_resource_and_ignores_inline(mod, sf):
    # Inline password is deliberately wrong/absent-equivalent to prove it's not used.
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    asset_def = _comp(mod, resource_key="snow", password=None, account="unused", user="unused").build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([asset_def], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct"
    assert conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset_def = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset_def])
