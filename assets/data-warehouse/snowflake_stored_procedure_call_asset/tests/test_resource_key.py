"""Committed tests for SnowflakeStoredProcedureCallAssetComponent's resource_key retrofit.

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
        self.sfqid = "fake-sfqid"

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def fetchone(self):
        return None

    @property
    def description(self):
        return []

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
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return rec


def _comp(mod, **kw):
    kw.setdefault("asset_key", "snowflake/procs/transform_customers")
    kw.setdefault("procedure_name", "TRANSFORM_CUSTOMERS")
    kw.setdefault("database", "DB")
    kw.setdefault("schema", "SC")
    kw.setdefault("account_env_var", "SNOWFLAKE_ACCOUNT")
    kw.setdefault("user_env_var", "SNOWFLAKE_USER")
    kw.setdefault("password_env_var", "SNOWFLAKE_PASSWORD")
    return mod.SnowflakeStoredProcedureCallAssetComponent(**kw)


def test_resource_key_default_none_uses_inline_env_vars(mod, sf):
    asset_def = _comp(mod).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "env_acct" and conn.kwargs["user"] == "env_user"
    assert conn.kwargs["password"] == "env_pw"
    assert conn.close_calls == 1
    assert conn.executed[0] == "CALL DB.SC.TRANSFORM_CUSTOMERS()"


def test_resource_path_uses_real_snowflake_resource_and_ignores_inline_env(mod, sf, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT")  # inline path would raise if it were used
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    asset_def = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([asset_def], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct" and conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1  # closed by the component (and again by the resource's own exit)
    assert conn.executed[0] == "CALL DB.SC.TRANSFORM_CUSTOMERS()"


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset_def = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset_def])
