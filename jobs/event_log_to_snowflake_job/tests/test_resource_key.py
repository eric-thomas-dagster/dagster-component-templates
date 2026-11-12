"""Committed tests for EventLogToSnowflakeJobComponent: resource_key retrofit.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection (aliased in the component module as `sc`). The resource path uses the REAL
dagster_snowflake.SnowflakeResource.get_connection(); the job runs for real under
execute_in_process().
"""
import dagster as dg
import pytest
from dagster_snowflake import SnowflakeResource

from .conftest import load_component_module


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def executemany(self, sql, rows):
        self.conn.executed.append((sql, rows))

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(mod, monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    # The component imports `snowflake.connector as sc` lazily inside the op body
    # on the inline (non-resource) path, so patch the real module's `connect`.
    import snowflake.connector

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)

    # No event-log rows: this test is only about the resource_key connection-wiring
    # retrofit (the op's "no new events" branch still opens + closes a connection,
    # which is all we need to exercise). Returning real EventLogEntry rows here hits
    # a pre-existing, unrelated bug in _row() (e.asset_key isn't a real attribute on
    # EventLogEntry) -- out of scope for this retrofit.
    monkeypatch.setattr(dg.DagsterInstance, "get_event_records", lambda self, *a, **k: [])
    return rec


def _comp(mod, **kw):
    kw.setdefault("job_name", "events_to_sf")
    kw.setdefault("schedule", None)
    kw.setdefault("account", "acct")
    kw.setdefault("user", "user")
    kw.setdefault("password_env", "SNOWFLAKE_PASSWORD")
    kw.setdefault("warehouse", "WH")
    kw.setdefault("database", "DB")
    return mod.EventLogToSnowflakeJobComponent(**kw)


def _job(mod, **kw):
    return _comp(mod, **kw).build_defs(None).jobs[0]


def test_resource_key_default_none_uses_inline_fields_and_closes_connection(mod, sf, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    job = _job(mod)
    op = job.graph.node_defs[0]
    assert op.required_resource_keys - {"io_manager"} == set()
    result = job.execute_in_process(instance=dg.DagsterInstance.ephemeral())
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "acct"
    assert conn.kwargs["user"] == "user"
    assert conn.kwargs["password"] == "env_pw"
    assert conn.close_calls == 1


def test_resource_key_set_uses_real_snowflake_resource_and_ignores_inline(mod, sf, monkeypatch):
    # No SNOWFLAKE_PASSWORD env set -- the inline path would raise if it were used.
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    job = _job(mod, resource_key="snow")
    op = job.graph.node_defs[0]
    assert op.required_resource_keys - {"io_manager"} == {"snow"}
    result = job.execute_in_process(instance=dg.DagsterInstance.ephemeral(), resources={"snow": res})
    assert result.success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct"
    assert conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1


def test_resource_key_not_registered_fails_loudly(mod, sf):
    job = _job(mod, resource_key="missing_key")
    with pytest.raises(Exception, match="missing_key"):
        job.execute_in_process(instance=dg.DagsterInstance.ephemeral())
