"""Committed test for SnowflakeCortexAssetComponent's resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection/cursor. The resource path uses the REAL dagster_snowflake.SnowflakeResource
.get_connection(); the component runs for real under dg.materialize().
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

    def fetchone(self):
        return (42,)

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
    kw.setdefault("asset_name", "cortex_out")
    kw.setdefault("snowflake_database", "DB")
    kw.setdefault("source_table", "SRC")
    kw.setdefault("target_table", "TGT")
    kw.setdefault("text_column", "body")
    kw.setdefault("cortex_function", "SENTIMENT")
    return mod.SnowflakeCortexAssetComponent(**kw)


def test_resource_key_default_none_uses_inline_env_vars(mod, sf):
    asset = _comp(mod).build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([asset]).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "env_acct" and conn.kwargs["user"] == "env_user"
    assert conn.kwargs["password"] == "env_pw" and conn.kwargs["database"] == "DB"
    assert conn.close_calls == 1
    assert any("SNOWFLAKE.CORTEX.SENTIMENT" in sql for sql in conn.executed)


def test_resource_path_uses_real_snowflake_resource_and_ignores_env(mod, sf, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT")  # inline path would raise if it were used
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    asset = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert asset.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([asset], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct" and conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1  # closed by the component (and again by the resource's own exit)
    assert any("SNOWFLAKE.CORTEX.SENTIMENT" in sql for sql in conn.executed)


def test_resource_key_not_registered_fails_loudly(mod, sf):
    asset = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset])
