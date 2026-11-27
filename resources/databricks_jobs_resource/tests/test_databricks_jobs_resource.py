"""Real, committed tests for DatabricksJobsResource.

Mocks only the external boundary (a fake databricks-sdk WorkspaceClient's
`.jobs.run_now()`/`.jobs.submit()`/`.jobs.repair_run()`/`.jobs.list()`/
`.jobs.wait_get_run_job_terminated_or_skipped()`, matching the real response
shapes verified against the installed databricks-sdk) -- everything else
(kwargs assembly, job-name resolution, success/failure handling) is real.
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


class _FakeBaseJob:
    def __init__(self, job_id):
        self.job_id = job_id


class FakeJobsAPI:
    def __init__(self, final_run=None, jobs_by_name=None):
        self.run_now_calls = []
        self.wait_calls = []
        self.list_calls = []
        self.submit_calls = []
        self.repair_run_calls = []
        self._final_run = final_run or _FakeRun()
        self._run_id = 999
        self._jobs_by_name = jobs_by_name or {}

    def run_now(self, **kwargs):
        self.run_now_calls.append(kwargs)
        return _FakeWait(self._run_id)

    def submit(self, **kwargs):
        self.submit_calls.append(kwargs)
        return _FakeWait(self._run_id)

    def repair_run(self, **kwargs):
        self.repair_run_calls.append(kwargs)
        return _FakeWait(self._run_id)

    def list(self, name=None, **kwargs):
        self.list_calls.append({"name": name, **kwargs})
        return iter([_FakeBaseJob(job_id) for job_id in self._jobs_by_name.get(name, [])])

    def wait_get_run_job_terminated_or_skipped(self, **kwargs):
        self.wait_calls.append(kwargs)
        return self._final_run


class FakeDatabricksClient:
    def __init__(self, final_run=None, jobs_by_name=None):
        self.jobs = FakeJobsAPI(final_run=final_run, jobs_by_name=jobs_by_name)


def _resource(monkeypatch, client, **kw):
    args = dict(workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN")
    args.update(kw)
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    resource = mod.DatabricksJobsResource(**args)
    monkeypatch.setattr(mod.DatabricksJobsResource, "_client", lambda self: client)
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


def test_jar_params_and_spark_submit_params_are_passed_through(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.run_now_and_wait(job_id=123, jar_params=["a", "b"], spark_submit_params=["--class", "Main"])
    call = client.jobs.run_now_calls[0]
    assert call["jar_params"] == ["a", "b"]
    assert call["spark_submit_params"] == ["--class", "Main"]


def test_job_name_resolves_to_single_job_id(monkeypatch):
    client = FakeDatabricksClient(jobs_by_name={"nightly-etl": [456]})
    resource = _resource(monkeypatch, client)
    result = resource.run_now_and_wait(job_name="nightly-etl")
    assert client.jobs.list_calls[0]["name"] == "nightly-etl"
    assert client.jobs.run_now_calls[0]["job_id"] == 456
    assert result["run_id"] == 999


def test_job_name_with_no_matches_raises(monkeypatch):
    client = FakeDatabricksClient(jobs_by_name={})
    resource = _resource(monkeypatch, client)
    with pytest.raises(ValueError, match="No Databricks job named"):
        resource.run_now_and_wait(job_name="does-not-exist")


def test_job_name_with_multiple_matches_raises(monkeypatch):
    client = FakeDatabricksClient(jobs_by_name={"dup": [1, 2]})
    resource = _resource(monkeypatch, client)
    with pytest.raises(ValueError, match="2 Databricks jobs are named"):
        resource.run_now_and_wait(job_name="dup")


def test_job_id_and_job_name_together_raises(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    with pytest.raises(ValueError, match="only one of"):
        resource.run_now_and_wait(job_id=1, job_name="x")


def test_missing_job_id_and_job_name_raises(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    with pytest.raises(ValueError, match="job_id or job_name is required"):
        resource.run_now_and_wait()


def test_submit_run_and_wait_converts_tasks_and_waits(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="SUCCESS"))
    resource = _resource(monkeypatch, client)
    run_json = {
        "run_name": "ad-hoc-run",
        "tasks": [{"task_key": "t1", "notebook_task": {"notebook_path": "/Workspace/nb"}}],
        "timeout_seconds": 600,
    }
    result = resource.submit_run_and_wait(run_json)
    assert result["run_id"] == 999
    assert result["result_state"] == "SUCCESS"
    submit_call = client.jobs.submit_calls[0]
    assert submit_call["run_name"] == "ad-hoc-run"
    assert submit_call["timeout_seconds"] == 600
    assert submit_call["tasks"][0].task_key == "t1"
    assert client.jobs.wait_calls[0]["run_id"] == 999


def test_submit_run_and_wait_raises_on_failure(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="FAILED", state_message="bad task"))
    resource = _resource(monkeypatch, client)
    with pytest.raises(dg.Failure, match="bad task"):
        resource.submit_run_and_wait({"run_name": "x", "tasks": []})


def test_repair_run_and_wait_passes_rerun_tasks_and_waits(monkeypatch):
    client = FakeDatabricksClient(final_run=_FakeRun(result_state="SUCCESS"))
    resource = _resource(monkeypatch, client)
    result = resource.repair_run_and_wait(run_id=777, rerun_tasks=["failed_task"])
    assert result["result_state"] == "SUCCESS"
    assert client.jobs.repair_run_calls[0]["run_id"] == 777
    assert client.jobs.repair_run_calls[0]["rerun_tasks"] == ["failed_task"]
    assert client.jobs.wait_calls[0]["run_id"] == 999


def test_repair_run_and_wait_rerun_all_failed_tasks(monkeypatch):
    client = FakeDatabricksClient()
    resource = _resource(monkeypatch, client)
    resource.repair_run_and_wait(run_id=777, rerun_all_failed_tasks=True)
    assert client.jobs.repair_run_calls[0]["rerun_all_failed_tasks"] is True


def test_component_registers_resource_under_key(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    comp = mod.DatabricksJobsResourceComponent(
        workspace_host="https://dbc.example", workspace_token_env_var="DATABRICKS_TOKEN",
    )
    defs = comp.build_defs(None)
    assert "databricks_jobs_resource" in defs.resources
    assert isinstance(defs.resources["databricks_jobs_resource"], mod.DatabricksJobsResource)
