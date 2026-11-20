"""Real, committed tests for TaskChainAssetComponent -- real dg.materialize() throughout, real
subprocess calls for command steps (free/local, matching this repo's "mock only the paid/external
call" rule), a fake DBAPI connection for sql steps (same fake as sql_asset's own tests), and real
module-level fixture callables for python steps (same convention as python_callable_asset's own
tests).
"""
import importlib

import dagster as dg
import pytest

from .conftest import load_component_module

FIXTURES_PATH = "assets.infrastructure.task_chain_asset.tests.fixture_callables"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def _materialize(component, **kw):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], **kw), asset_def


def _step_results(result, asset_def):
    return result.asset_materializations_for_node(asset_def.op.name)[0].metadata["step_results"].value


# --- construction / validation ---------------------------------------------------------------

def test_requires_at_least_one_step(mod):
    with pytest.raises(ValueError, match="at least one entry"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[])


def test_step_requires_an_id(mod):
    with pytest.raises(ValueError, match="missing a"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"type": "command", "command": "echo hi"}])


def test_duplicate_step_ids_rejected(mod):
    with pytest.raises(ValueError, match="duplicate step id"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
            {"id": "a", "type": "command", "command": "echo 1"},
            {"id": "a", "type": "command", "command": "echo 2"},
        ])


def test_unknown_step_type_rejected(mod):
    with pytest.raises(ValueError, match="must be one of"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "bogus"}])


def test_command_step_requires_command(mod):
    with pytest.raises(ValueError, match="requires `command`"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "command"}])


def test_sql_step_requires_resource_key_or_connection_env(mod):
    with pytest.raises(ValueError, match="requires `resource_key` or `connection_env`"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "sql", "sql": "SELECT 1"}])


def test_sql_step_rejects_both_resource_key_and_connection_env(mod):
    with pytest.raises(ValueError, match="at most one"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
            {"id": "a", "type": "sql", "resource_key": "db", "connection_env": "DB_URL", "sql": "SELECT 1"},
        ])


def test_sql_step_neither_resource_key_nor_connection_env_ok_when_chain_dry_run(mod):
    # Does not raise -- dry_run never opens a connection, so a sql step needs neither.
    mod.TaskChainAssetComponent(asset_key="infra/t", dry_run=True, steps=[{"id": "a", "type": "sql", "sql": "SELECT 1"}])


def test_sql_step_requires_exactly_one_sql_source(mod):
    with pytest.raises(ValueError, match="exactly one"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
            {"id": "a", "type": "sql", "resource_key": "db", "sql": "SELECT 1", "sql_statements": ["SELECT 1"]},
        ])


def test_python_step_requires_callable_path(mod):
    with pytest.raises(ValueError, match="requires `callable_path`"):
        mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "python"}])


# --- command steps -----------------------------------------------------------------------------

def test_two_command_steps_run_in_order(mod, tmp_path):
    marker = tmp_path / "order.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "first", "type": "command", "command": f"echo 1 >> {marker}"},
        {"id": "second", "type": "command", "command": f"echo 2 >> {marker}"},
    ])
    result, asset_def = _materialize(component)
    assert result.success
    assert marker.read_text() == "1\n2\n"
    results = _step_results(result, asset_def)
    assert [r["id"] for r in results] == ["first", "second"]
    assert all(r["status"] == "success" for r in results)


def test_a_failing_command_step_stops_the_chain(mod, tmp_path):
    marker = tmp_path / "never.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "bad", "type": "command", "command": "exit 7"},
        {"id": "never", "type": "command", "command": f"touch {marker}"},
    ])
    result, _ = _materialize(component, raise_on_error=False)
    assert not result.success
    assert not marker.exists()


def test_success_exit_codes_on_a_command_step_counts_as_success(mod):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "command", "command": "exit 5", "success_exit_codes": [5]},
    ])
    result, asset_def = _materialize(component)
    assert result.success
    assert _step_results(result, asset_def)[0]["exit_code"] == 5


def test_skip_on_exit_code_on_a_command_step_is_recorded(mod):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "command", "command": "exit 99", "skip_on_exit_code": 99},
    ])
    result, asset_def = _materialize(component)
    assert result.success
    assert _step_results(result, asset_def)[0]["skipped"] is True


def test_partition_key_is_substituted_into_command(mod, tmp_path):
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", partition_type="static", partition_values="2026-01-01,2026-01-02",
        steps=[{"id": "a", "type": "command", "command": f"echo {{partition_key}} > {tmp_path}/{{partition_key}}.txt"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], partition_key="2026-01-01")
    assert result.success
    assert (tmp_path / "2026-01-01.txt").exists()


# --- sql steps -----------------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, parameters=None):
        self.conn.executed.append({"sql": sql, "parameters": parameters})

    def close(self):
        pass


class _FakeConn:
    def __init__(self):
        self.executed = []
        self.commit_calls = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commit_calls += 1

    def close(self):
        pass


class _RawConnectionResource:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        return self._conn


def test_sql_step_runs_against_a_resource(mod):
    conn = _FakeConn()
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "sql", "resource_key": "db", "sql": "DELETE FROM stale_rows"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"db": _RawConnectionResource(conn)})
    assert result.success
    assert conn.executed[0]["sql"] == "DELETE FROM stale_rows"
    assert conn.commit_calls == 1


def test_defs_load_cleanly_with_dry_run_and_sql_step_resource_not_registered(mod):
    """The real bug: a sql step's resource_key forces Dagster to validate the resource is
    registered at REPOSITORY-DEF resolution time (dg check defs / dg dev), not at run time --
    confirmed live against plain Dagster. dry_run sidesteps this entirely: no
    required_resource_keys is declared at all when the chain's own dry_run is True."""
    component = mod.TaskChainAssetComponent(asset_key="infra/t", dry_run=True, steps=[
        {"id": "a", "type": "sql", "resource_key": "db", "sql": "SELECT 1"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    repo = dg.Definitions(assets=[asset_def]).get_repository_def()  # must not raise
    assert repo is not None


def test_defs_load_cleanly_with_connection_env_sql_step_and_no_resource_registered(mod):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "sql", "connection_env": "DB_URL", "sql": "SELECT 1"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    repo = dg.Definitions(assets=[asset_def]).get_repository_def()  # must not raise
    assert repo is not None


def test_defs_fail_to_load_with_resource_key_sql_step_and_no_resource_registered(mod):
    """Documents the real, unavoidable Dagster constraint (not a regression) -- see
    SqlAssetComponent's own README for why resource_key can't avoid this."""
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "sql", "resource_key": "db", "sql": "SELECT 1"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="db"):
        dg.Definitions(assets=[asset_def]).get_repository_def()


def test_sql_step_connection_env_runs_real_sql_via_sqlalchemy(mod, monkeypatch):
    monkeypatch.setenv("TASK_CHAIN_TEST_DB_URL", "sqlite:///:memory:")
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "sql", "connection_env": "TASK_CHAIN_TEST_DB_URL", "sql_statements": ["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1)"]},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])  # no `resources=` at all -- connection_env bypasses Dagster's resource system
    assert result.success


def test_sql_step_connection_env_missing_env_var_fails_at_run_time_not_load_time(mod, monkeypatch):
    monkeypatch.delenv("TASK_CHAIN_TEST_DB_URL_UNSET", raising=False)
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "sql", "connection_env": "TASK_CHAIN_TEST_DB_URL_UNSET", "sql": "SELECT 1"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    dg.Definitions(assets=[asset_def]).get_repository_def()  # loads fine -- the failure is deferred
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success


def test_a_failing_sql_step_stops_the_chain(mod, tmp_path):
    class _BoomConn:
        def cursor(self):
            class _C:
                def execute(self, *a, **kw):
                    raise RuntimeError("syntax error")

                def close(self):
                    pass
            return _C()

        def close(self):
            pass

    marker = tmp_path / "never.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "bad_sql", "type": "sql", "resource_key": "db", "sql": "NOT REAL SQL"},
        {"id": "never", "type": "command", "command": f"touch {marker}"},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"db": _RawConnectionResource(_BoomConn())}, raise_on_error=False)
    assert not result.success
    assert not marker.exists()


# --- python steps --------------------------------------------------------------------------------

def test_python_step_calls_the_real_callable(mod, fx):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "a", "type": "python", "callable_path": f"{FIXTURES_PATH}:record_args_kwargs", "args": [1, 2], "kwargs": {"x": "y"}},
    ])
    result, asset_def = _materialize(component)
    assert result.success
    assert fx.CALLS["args"] == (1, 2)
    assert fx.CALLS["kwargs"] == {"x": "y"}


def test_a_failing_python_step_stops_the_chain(mod, fx, tmp_path):
    marker = tmp_path / "never.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "boom", "type": "python", "callable_path": f"{FIXTURES_PATH}:always_raises"},
        {"id": "never", "type": "command", "command": f"touch {marker}"},
    ])
    result, _ = _materialize(component, raise_on_error=False)
    assert not result.success
    assert not marker.exists()


# --- mixed chain ----------------------------------------------------------------------------------

def test_mixed_command_sql_python_chain_runs_in_order(mod, fx, tmp_path):
    conn = _FakeConn()
    marker = tmp_path / "ran.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[
        {"id": "extract", "type": "command", "command": f"touch {marker}"},
        {"id": "load", "type": "sql", "resource_key": "db", "sql": "INSERT INTO t VALUES (1)"},
        {"id": "notify", "type": "python", "callable_path": f"{FIXTURES_PATH}:record_args_kwargs", "kwargs": {"status": "done"}},
    ])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"db": _RawConnectionResource(conn)})
    assert result.success
    assert marker.exists()
    assert conn.executed[0]["sql"] == "INSERT INTO t VALUES (1)"
    assert fx.CALLS["kwargs"] == {"status": "done"}
    results = _step_results(result, asset_def)
    assert [r["id"] for r in results] == ["extract", "load", "notify"]


# --- dry_run ----------------------------------------------------------------------------------

def test_dry_run_skips_every_step(mod, fx, tmp_path):
    marker = tmp_path / "never.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", dry_run=True, steps=[
        {"id": "a", "type": "command", "command": f"touch {marker}"},
        {"id": "b", "type": "python", "callable_path": f"{FIXTURES_PATH}:record_args_kwargs"},
    ])
    result, asset_def = _materialize(component)
    assert result.success
    assert not marker.exists()
    assert "args" not in fx.CALLS
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is True
    assert "step_results" not in md


def test_dry_run_false_by_default(mod):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "command", "command": "echo hi"}])
    result, asset_def = _materialize(component)
    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["dry_run"].value is False


# --- run_window ----------------------------------------------------------------------------------

def test_run_window_outside_the_window_skips_cleanly(mod, tmp_path):
    marker = tmp_path / "never.txt"
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", run_window="00:00-00:01 UTC",
        steps=[{"id": "a", "type": "command", "command": f"touch {marker}"}],
    )
    result, asset_def = _materialize(component)
    assert result.success  # skip, not fail
    assert not marker.exists()
    assert result.asset_materializations_for_node(asset_def.op.name) == []


def test_run_window_covering_all_day_always_runs(mod, tmp_path):
    marker = tmp_path / "ran.txt"
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", run_window="00:00-23:59 UTC",
        steps=[{"id": "a", "type": "command", "command": f"touch {marker}"}],
    )
    result, _ = _materialize(component)
    assert result.success
    assert marker.exists()


def test_run_window_unset_always_runs(mod, tmp_path):
    marker = tmp_path / "ran.txt"
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "command", "command": f"touch {marker}"}])
    result, _ = _materialize(component)
    assert result.success
    assert marker.exists()


def test_run_window_malformed_spec_fails_open_and_still_runs(mod, tmp_path):
    marker = tmp_path / "ran.txt"
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", run_window="not a valid window spec",
        steps=[{"id": "a", "type": "command", "command": f"touch {marker}"}],
    )
    result, _ = _materialize(component)
    assert result.success
    assert marker.exists()


# --- pool / retry_policy / timeout_seconds -------------------------------------------------------

def test_no_pool_or_retry_policy_by_default(mod):
    component = mod.TaskChainAssetComponent(asset_key="infra/t", steps=[{"id": "a", "type": "command", "command": "echo hi"}])
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool is None
    assert asset_def.op.retry_policy is None


def test_pool_and_retry_policy_wire_through(mod):
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", pool="rmj_migration", retry_policy_max_retries=2, retry_policy_delay_seconds=5,
        steps=[{"id": "a", "type": "command", "command": "echo hi"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "rmj_migration"
    assert asset_def.op.retry_policy.max_retries == 2
    assert asset_def.op.retry_policy.delay == 5


def test_timeout_seconds_fails_a_slow_chain(mod):
    component = mod.TaskChainAssetComponent(
        asset_key="infra/t", timeout_seconds=1,
        steps=[{"id": "a", "type": "command", "command": "sleep 2"}],
    )
    result, _ = _materialize(component, raise_on_error=False)
    assert not result.success
