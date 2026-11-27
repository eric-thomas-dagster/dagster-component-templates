"""Real, committed tests for DatabricksJobsResource.

Mocks only the external boundary (a fake databricks-sdk WorkspaceClient's
`.jobs.run_now()`/`.jobs.wait_get_run_job_terminated_or_skipped()`, matching
the real response shapes verified against the installed databricks-sdk) --
everything else (run_now kwargs assembly, success/failure handling) is real.
"""
import pytest
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


def _resource(monkeypatch, client, **kw):
    args = dict(workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN")
    args.update(kw)
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    resource = mod.DatabricksJobsResource(**args)
    monkeypatch.setattr(resource, "_client", lambda: client)
    return resource


def test_successful_run_now_returns_result(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="SUCCESS"))
    resource = _resource(monkeypatch, client)
    result = resource.run_now_and_wait(job_id=123)
    assert result["run_id"] == 999
    assert result["result_state"] == "SUCCESS"
    assert client.jobs.run_now_calls[0]["job_id"] == 123
    assert client.jobs.wait_calls[0]["run_id"] == 999


def test_failed_run_raises_by_default(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="FAILED", state_message="boom"))
    resource = _resource(monkeypatch, client)
    with pytest.raises(dg.Failure) as exc:
        resource.run_now_and_wait(job_id=123)
    assert "boom" in str(exc.value)
    assert "FAILED" in str(exc.value)


def test_failed_run_does_not_raise_when_disabled(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="FAILED", state_message="boom"))
    resource = _resource(monkeypatch, client)
    result = resource.run_now_and_wait(job_id=123, fail_on_non_success=False)
    assert result["result_state"] == "FAILED"


def test_job_parameters_are_coerced_to_strings_and_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.run_now_and_wait(job_id=123, job_parameters={"run_date": "2026-01-15", "retries": 3})
    assert client.jobs.run_now_calls[0]["job_parameters"] == {"run_date": "2026-01-15", "retries": "3"}


def test_notebook_params_and_python_params_are_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.run_now_and_wait(job_id=123, notebook_params={"env": "prod"}, python_params=["--verbose"])
    call = client.jobs.run_now_calls[0]
    assert call["notebook_params"] == {"env": "prod"}
    assert call["python_params"] == ["--verbose"]


def test_idempotency_token_is_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.run_now_and_wait(job_id=123, idempotency_token="my-token")
    assert client.jobs.run_now_calls[0]["idempotency_token"] == "my-token"


def test_poll_timeout_seconds_is_passed_to_wait(monkeypatch):
    from datetime import timedelta

    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.run_now_and_wait(job_id=123, poll_timeout_seconds=42)
    assert client.jobs.wait_calls[0]["timeout"] == timedelta(seconds=42)


def test_missing_token_env_var_raises_actionable_error(monkeypatch):
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    resource = mod.DatabricksJobsResource(workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN")
    with pytest.raises(KeyError, match="DATABRICKS_TOKEN"):
        resource.run_now_and_wait(job_id=123)


def test_timedout_run_raises_with_state_in_message(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(run_page_url="https://dbc.example/run/999", result_state="TIMEDOUT"))
    resource = _resource(monkeypatch, client)
    with pytest.raises(dg.Failure, match="TIMEDOUT"):
        resource.run_now_and_wait(job_id=123)


def test_component_registers_resource_under_key(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    comp = mod.DatabricksJobsResourceComponent(
        workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN",
    )
    defs = comp.build_defs(None)
    assert "databricks_jobs_resource" in defs.resources
    assert isinstance(defs.resources["databricks_jobs_resource"], mod.DatabricksJobsResource)
