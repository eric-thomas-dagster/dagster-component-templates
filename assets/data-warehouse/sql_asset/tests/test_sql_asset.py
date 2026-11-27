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
        self.description = None
        self._rows = []

    def execute(self, sql, parameters=None):
        self.conn.executed.append({"sql": sql, "parameters": parameters})
        hit = self.conn.rows_by_statement.get(sql)
        if hit is not None:
            columns, rows = hit
            self.description = [(c,) for c in columns]
            self._rows = rows
        else:
            self.description = None
            self._rows = []

    def fetchmany(self, n):
        return self._rows[:n]

    def close(self):
        self.conn.cursor_closed = True


class FakeConn:
    def __init__(self, commit_raises=False, rows_by_statement=None):
        self.executed = []
        self.commit_calls = 0
        self.close_calls = 0
        self.cursor_closed = False
        self._commit_raises = commit_raises
        self.rows_by_statement = rows_by_statement or {}

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


# --- pool / retry_policy / timeout_seconds -------------------------------------------------

def test_no_pool_or_retry_policy_by_default(mod):
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    assert asset_def.op.pool is None
    assert asset_def.op.retry_policy is None


def test_pool_wires_through(mod):
    asset_def = _comp(mod, sql="SELECT 1", pool="warehouse_concurrency").build_defs(None).assets[0]
    assert asset_def.op.pool == "warehouse_concurrency"


def test_retry_policy_wires_through(mod):
    asset_def = _comp(
        mod, sql="SELECT 1", retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    ).build_defs(None).assets[0]
    assert asset_def.op.retry_policy.max_retries == 3
    assert asset_def.op.retry_policy.delay == 2


class _SlowFakeCursor(FakeCursor):
    def execute(self, sql, parameters=None):
        import time as _time
        _time.sleep(0.2)
        super().execute(sql, parameters)


class _SlowFakeConn(FakeConn):
    def cursor(self):
        return _SlowFakeCursor(self)


def test_timeout_seconds_fails_a_slow_query(mod):
    conn = _SlowFakeConn()
    asset_def = _comp(mod, sql="SELECT 1", timeout_seconds=0.01).build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}, raise_on_error=False)
    assert not result.success


def test_timeout_seconds_unset_does_not_bound_a_slow_query(mod):
    conn = _SlowFakeConn()
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success


# --- run_window ------------------------------------------------------------------------------

def test_run_window_outside_the_window_skips_cleanly(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1", run_window="00:00-00:01 UTC").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success  # skip, not fail
    assert conn.executed == []
    assert result.asset_materializations_for_node(asset_def.op.name) == []


def test_run_window_covering_all_day_always_runs(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1", run_window="00:00-23:59 UTC").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    assert conn.executed[0]["sql"] == "SELECT 1"


def test_run_window_unset_always_runs(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    assert conn.executed[0]["sql"] == "SELECT 1"


def test_run_window_malformed_spec_fails_open_and_still_runs(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1", run_window="not a valid window spec").build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    assert conn.executed[0]["sql"] == "SELECT 1"


# --- connection_env / dry_run: lazy connection, no eager required_resource_keys -----------------

def test_resource_key_and_connection_env_are_mutually_exclusive(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.SqlAssetComponent(asset_key="t", sql="SELECT 1")  # neither
    with pytest.raises(Exception, match="exactly one"):
        mod.SqlAssetComponent(asset_key="t", sql="SELECT 1", resource_key="db", connection_env="DB_URL")  # both


def test_dry_run_requires_neither_resource_key_nor_connection_env(mod):
    # Does not raise -- dry_run never opens a connection, so neither is required.
    mod.SqlAssetComponent(asset_key="t", sql="SELECT 1", dry_run=True)


def test_defs_load_cleanly_with_dry_run_and_no_resource_registered_anywhere(mod):
    """The actual bug: resource_key's required_resource_keys forces Dagster to validate the
    resource is registered at REPOSITORY-DEF resolution time (dg check defs / dg dev), not at
    run time -- confirmed live against plain Dagster, not assumed. dry_run sidesteps this
    entirely by never declaring required_resource_keys."""
    asset_def = _comp(mod, sql="SELECT 1", resource_key=None, dry_run=True).build_defs(None).assets[0]
    repo = dg.Definitions(assets=[asset_def]).get_repository_def()  # must not raise
    assert repo is not None


def test_defs_load_cleanly_with_connection_env_and_no_resource_registered_anywhere(mod):
    asset_def = _comp(mod, sql="SELECT 1", resource_key=None, connection_env="DB_URL").build_defs(None).assets[0]
    repo = dg.Definitions(assets=[asset_def]).get_repository_def()  # must not raise
    assert repo is not None


def test_defs_fail_to_load_with_resource_key_and_no_resource_registered_anywhere(mod):
    """Documents the real, unavoidable Dagster constraint (not a regression): using resource_key
    (without dry_run) still requires the resource to load the repository, because
    context.resources only ever binds a resource an op/asset actually declared via
    required_resource_keys -- confirmed live, no supported way around it for a dynamically-named
    resource. connection_env or dry_run are the ways to avoid this specific requirement."""
    asset_def = _comp(mod, sql="SELECT 1").build_defs(None).assets[0]  # resource_key="db" (default), no dry_run
    with pytest.raises(Exception, match="db"):
        dg.Definitions(assets=[asset_def]).get_repository_def()


def test_dry_run_logs_sql_and_never_touches_a_connection(mod):
    asset_def = _comp(mod, sql="SELECT 1 FROM orders", resource_key=None, dry_run=True).build_defs(None).assets[0]
    result = dg.materialize([asset_def])  # no resources at all -- would fail immediately if a connection were attempted
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is True
    assert md["statements"].value == ["SELECT 1 FROM orders"]


def test_dry_run_with_pre_sql_and_sql_statements_logs_all_of_them(mod):
    asset_def = _comp(
        mod, sql=None, sql_statements=["DELETE FROM a", "INSERT INTO a VALUES (1)"],
        pre_sql=["SET search_path TO analytics"], resource_key=None, dry_run=True,
    ).build_defs(None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["statements"].value == ["DELETE FROM a", "INSERT INTO a VALUES (1)"]


def test_connection_env_runs_real_sql_via_sqlalchemy(mod, monkeypatch):
    monkeypatch.setenv("SQL_ASSET_TEST_DB_URL", "sqlite:///:memory:")
    asset_def = _comp(
        mod, sql=None, sql_statements=["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1)"],
        resource_key=None, connection_env="SQL_ASSET_TEST_DB_URL",
    ).build_defs(None).assets[0]
    result = dg.materialize([asset_def])  # no `resources=` at all -- connection_env bypasses Dagster's resource system
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["connection_env"].value == "SQL_ASSET_TEST_DB_URL"
    assert md["statement_count"].value == 2


def test_connection_env_missing_env_var_fails_at_run_time_not_load_time(mod, monkeypatch):
    monkeypatch.delenv("SQL_ASSET_TEST_DB_URL_UNSET", raising=False)
    asset_def = _comp(mod, sql="SELECT 1", resource_key=None, connection_env="SQL_ASSET_TEST_DB_URL_UNSET").build_defs(None).assets[0]
    dg.Definitions(assets=[asset_def]).get_repository_def()  # loads fine -- the failure is deferred

    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success
    failure_data = result.failure_data_for_node(asset_def.op.name)
    assert "SQL_ASSET_TEST_DB_URL_UNSET" in failure_data.error.message
    assert "is empty/unset" in failure_data.error.message


def test_positional_parameters_passed_through(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="DELETE FROM a WHERE d < %s", parameters=["2026-01-01"]).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.executed[0]["parameters"] == ["2026-01-01"]


def test_commit_per_statement_commits_after_each(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql_statements=["DELETE FROM a", "INSERT INTO a VALUES (1)"], commit_per_statement=True).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.commit_calls == 2  # one per statement, not one final commit


def test_commit_per_statement_also_commits_pre_sql(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1", pre_sql=["SET x = 1"], commit_per_statement=True).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.commit_calls == 2  # pre_sql statement + the one main statement


def test_default_commit_is_once_at_the_end(mod):
    conn = FakeConn()
    asset_def = _comp(mod, sql_statements=["DELETE FROM a", "INSERT INTO a VALUES (1)"]).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert conn.commit_calls == 1


def test_fetch_last_statement_rows_returns_the_final_selects_rows(mod):
    sql = "CREATE TABLE a (id int); SELECT id FROM a"
    conn = FakeConn(rows_by_statement={
        "SELECT id FROM a": (["id"], [(1,), (2,)]),
    })
    asset_def = _comp(mod, sql=sql, split_statements=True, fetch_last_statement_rows=True).build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    rows = md["last_statement_rows"].value
    assert rows == [{"id": 1}, {"id": 2}]


def test_fetch_last_statement_rows_off_by_default(mod):
    sql = "CREATE TABLE a (id int); SELECT id FROM a"
    conn = FakeConn(rows_by_statement={"SELECT id FROM a": (["id"], [(1,)])})
    asset_def = _comp(mod, sql=sql, split_statements=True).build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)})
    assert result.success
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert "last_statement_rows" not in md


def test_split_statements_runs_one_execute_per_statement(mod):
    conn = FakeConn()
    sql = "DELETE FROM a; INSERT INTO a VALUES (1); INSERT INTO a VALUES (2)"
    asset_def = _comp(mod, sql=sql, split_statements=True).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert [e["sql"] for e in conn.executed] == ["DELETE FROM a;", "INSERT INTO a VALUES (1);", "INSERT INTO a VALUES (2)"]


def test_split_statements_is_string_literal_aware(mod):
    """A naive ';'.split() would break on a semicolon inside a string literal -- sqlparse.split() doesn't."""
    conn = FakeConn()
    sql = "INSERT INTO a (note) VALUES ('a;b'); SELECT 1"
    asset_def = _comp(mod, sql=sql, split_statements=True).build_defs(None).assets[0]
    assert dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}).success
    assert len(conn.executed) == 2
    assert conn.executed[0]["sql"] == "INSERT INTO a (note) VALUES ('a;b');"


def test_split_statements_rejected_with_sql_statements(mod):
    with pytest.raises(Exception, match="split_statements"):
        _comp(mod, sql_statements=["SELECT 1"], split_statements=True)


def test_split_statements_without_sqlparse_fails_clearly(mod, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == "sqlparse":
            raise ImportError("No module named 'sqlparse'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)
    conn = FakeConn()
    asset_def = _comp(mod, sql="SELECT 1; SELECT 2", split_statements=True).build_defs(None).assets[0]
    result = dg.materialize([asset_def], resources={"db": RawConnectionResource(conn)}, raise_on_error=False)
    assert not result.success
    failure_data = result.failure_data_for_node(asset_def.op.name)
    assert "sqlparse" in failure_data.error.message
