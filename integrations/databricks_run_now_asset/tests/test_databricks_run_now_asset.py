"""Real, committed tests for DatabricksRunNowAssetComponent.

Mocks only the external boundary (a fake databricks-sdk WorkspaceClient's
`.jobs.run_now()`/`.jobs.wait_get_run_job_terminated_or_skipped()`, matching
the real response shapes verified against the installed databricks-sdk:
`run_now(...) -> Wait[Run]` whose `.response` is a `RunNowResponse(run_id=...)`,
and `wait_get_run_job_terminated_or_skipped(run_id=...) -> Run` whose `.state`
is a `RunState(life_cycle_state, result_state, state_message)`) -- everything
else (run_now kwargs assembly, success/failure handling, Dagster asset wiring)
is real.
"""
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()


class _FakeState:
    def __init__(self, life_cycle_state="TERMINATED", result_state="SUCCESS", state_message=None):
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message


class _FakeRun:
    def __init__(self, run_page_url="https://dbc.example/run/1", **state_kwargs):
        self.run_page_url = run_page_url
        self.state = _FakeState(**state_kwargs)


class _FakeWaitResponse:
    def __init__(self, run_id):
        self.run_id = run_id


class _FakeWait:
    def __init__(self, run_id):
        self.response = _FakeWaitResponse(run_id)


class FakeJobsAPI:
    def __init__(self, final_run=None):
        self.run_now_calls = []
        self.wait_calls = []
        self._final_run = final_run or _FakeRun()
        self._run_id = 999

    def run_now(self, **kwargs):
        self.run_now_calls.append(kwargs)
        return _FakeWait(self._run_id)

    def wait_get_run_job_terminated_or_skipped(self, **kwargs):
        self.wait_calls.append(kwargs)
        return self._final_run


class FakeDatabricksClient:
    def __init__(self, final_run=None):
        self.jobs = FakeJobsAPI(final_run=final_run)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_databricks_client", lambda host, token: client)
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")


def _comp(**kw):
    args = dict(asset_name="out", job_id=123, workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN")
    args.update(kw)
    return mod.DatabricksRunNowAssetComponent(**args)


def test_successful_run_now_materializes_with_metadata(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="SUCCESS"))
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["job_id"].value == 123
    assert meta["run_id"].value == 999
    assert meta["result_state"].text == "SUCCESS"

    assert client.jobs.run_now_calls[0]["job_id"] == 123
    assert client.jobs.wait_calls[0]["run_id"] == 999


def test_failed_run_raises_by_default(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="FAILED", state_message="boom"))
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "boom" in str(failure.event_specific_data.error)
    assert "FAILED" in str(failure.event_specific_data.error)


def test_failed_run_does_not_raise_when_disabled(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="FAILED", state_message="boom"))
    _patch_client(monkeypatch, client)
    comp = _comp(fail_on_non_success=False)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["result_state"].text == "FAILED"


def test_job_parameters_are_coerced_to_strings_and_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    _patch_client(monkeypatch, client)
    comp = _comp(job_parameters={"run_date": "2026-01-15", "retries": 3})
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.jobs.run_now_calls[0]["job_parameters"] == {"run_date": "2026-01-15", "retries": "3"}


def test_notebook_params_and_python_params_are_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    _patch_client(monkeypatch, client)
    comp = _comp(notebook_params={"env": "prod"}, python_params=["--verbose"])
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    call = client.jobs.run_now_calls[0]
    assert call["notebook_params"] == {"env": "prod"}
    assert call["python_params"] == ["--verbose"]


def test_idempotency_token_is_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    _patch_client(monkeypatch, client)
    comp = _comp(idempotency_token="my-token")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.jobs.run_now_calls[0]["idempotency_token"] == "my-token"


def test_poll_timeout_seconds_is_passed_to_wait(monkeypatch):
    from datetime import timedelta

    client = FakeDatabricksClient()
    _patch_client(monkeypatch, client)
    comp = _comp(poll_timeout_seconds=42)
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.jobs.wait_calls[0]["timeout"] == timedelta(seconds=42)


def test_missing_token_env_var_raises_actionable_error(monkeypatch):
    client = FakeDatabricksClient()
    monkeypatch.setattr(mod, "_make_databricks_client", lambda host, token: client)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "DATABRICKS_TOKEN" in str(failure.event_specific_data.error)


def test_timedout_run_raises_with_state_in_message(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(run_page_url="https://dbc.example/run/999", result_state="TIMEDOUT"))
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "TIMEDOUT" in str(failure.event_specific_data.error)
