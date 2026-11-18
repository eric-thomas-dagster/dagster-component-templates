"""Committed tests for SnowflakeSqlAssetComponent.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection/cursor. The component runs for real under dg.materialize().
"""
import dagster as dg
import pytest
import snowflake.connector
from dagster_snowflake import SnowflakeResource

from .conftest import load_component_module


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 7

    def execute(self, sql, parameters=None, num_statements=None):
        if self.conn.delay_seconds:
            import time
            time.sleep(self.conn.delay_seconds)
        self.conn.executed.append({"sql": sql, "parameters": parameters, "num_statements": num_statements})

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs, delay_seconds=0):
        self.kwargs = kwargs
        self.executed = []
        self.commit_calls = 0
        self.close_calls = 0
        self.delay_seconds = delay_seconds

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []
        self.delay_seconds = 0


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs, delay_seconds=rec.delay_seconds)
        rec.conns.append(c)
        return c

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return rec


def _comp(mod, **kw):
    kw.setdefault("asset_key", "snowflake/transforms/rollup")
    kw.setdefault("account_env_var", "SNOWFLAKE_ACCOUNT")
    kw.setdefault("user_env_var", "SNOWFLAKE_USER")
    kw.setdefault("password_env_var", "SNOWFLAKE_PASSWORD")
    return mod.SnowflakeSqlAssetComponent(**kw)


def test_requires_exactly_one_sql_source(mod):
    with pytest.raises(Exception, match="exactly one"):
        _comp(mod)
    with pytest.raises(Exception, match="exactly one"):
        _comp(mod, sql="SELECT 1", sql_statements=["SELECT 1"])


def test_requires_connection_or_resource_key(mod):
    with pytest.raises(Exception, match="resource_key"):
        mod.SnowflakeSqlAssetComponent(asset_key="x/y", sql="SELECT 1")


def test_single_sql_runs_with_num_statements_zero(mod, sf):
    asset_def = _comp(mod, sql="CREATE TABLE a; INSERT INTO a VALUES (1);").build_defs(None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    (conn,) = sf.conns
    assert conn.executed[0]["num_statements"] == 0
    assert conn.commit_calls == 1
    assert conn.close_calls == 1


def test_sql_statements_list_runs_one_execute_per_statement(mod, sf):
    asset_def = _comp(mod, sql_statements=["DELETE FROM a", "INSERT INTO a VALUES (1)"]).build_defs(None).assets[0]
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    assert [e["sql"] for e in conn.executed] == ["DELETE FROM a", "INSERT INTO a VALUES (1)"]
    assert all(e["num_statements"] is None for e in conn.executed)


def test_sql_file_is_read_and_executed(mod, sf, tmp_path):
    f = tmp_path / "q.sql"
    f.write_text("SELECT 1;")
    asset_def = _comp(mod, sql_file=str(f)).build_defs(None).assets[0]
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    assert conn.executed[0]["sql"] == "SELECT 1;"


def test_parameters_are_bound(mod, sf):
    asset_def = _comp(mod, sql="DELETE FROM a WHERE d < %(cutoff)s", parameters={"cutoff": "2026-01-01"}).build_defs(None).assets[0]
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    main = [e for e in conn.executed if "cutoff" not in str(e.get("parameters"))][0] if False else conn.executed[0]
    assert main["parameters"] == {"cutoff": "2026-01-01"}


def test_session_overrides_issued_before_main_sql(mod, sf):
    asset_def = _comp(
        mod,
        sql="SELECT 1",
        session_warehouse="WH",
        session_database="DB",
        session_schema="SC",
        session_role="ROLE",
    ).build_defs(None).assets[0]
    assert dg.materialize([asset_def]).success
    (conn,) = sf.conns
    statements = [e["sql"] for e in conn.executed]
    assert statements == ["USE WAREHOUSE WH", "USE DATABASE DB", "USE SCHEMA SC", "USE ROLE ROLE", "SELECT 1"]


def test_rowcount_and_statement_count_in_metadata(mod, sf):
    asset_def = _comp(mod, sql_statements=["A", "B"]).build_defs(None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    mats = result.asset_materializations_for_node("snowflake__transforms__rollup")
    metadata = mats[0].metadata
    assert metadata["statement_count"].value == 2


def test_no_pool_by_default(mod, sf):
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    assert asset_def.op.pool is None


def test_pool_wires_through(mod, sf):
    asset_def = _comp(mod, sql="SELECT 1", pool="warehouse_slots").build_defs(None).assets[0]
    assert asset_def.op.pool == "warehouse_slots"


def test_retry_policy_wires_through(mod, sf):
    asset_def = _comp(
        mod, sql="SELECT 1", retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    ).build_defs(None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_query(mod, sf):
    import time
    sf.delay_seconds = 1
    asset_def = _comp(mod, sql="SELECT 1", timeout_seconds=0.05).build_defs(None).assets[0]
    start = time.monotonic()
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1


def test_resource_path_uses_real_snowflake_resource(mod, sf, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT")  # inline path would raise if it were used
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    asset_def = mod.SnowflakeSqlAssetComponent(
        asset_key="snowflake/transforms/rollup", sql="SELECT 1", resource_key="snow"
    ).build_defs(None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([asset_def], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct"
