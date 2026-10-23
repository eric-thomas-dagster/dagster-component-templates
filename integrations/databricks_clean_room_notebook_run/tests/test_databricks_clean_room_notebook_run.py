"""Real, committed tests for DatabricksCleanRoomNotebookRunComponent.

Mocks only the external call: a fake WorkspaceClient.jobs whose
submit()/wait_get_run_job_terminated_or_skipped() return real
databricks.sdk.service.jobs dataclass instances (Run/RunState), not hand-
rolled dicts -- so a wrong attribute/enum name would fail the SAME way
against the real SDK. Everything else (request building, success/failure
handling, Dagster asset wiring) is real.
"""
import dagster as dg
import pytest
from databricks.sdk.service import jobs

from .conftest import load_component_module

mod = load_component_module()


class _FakeSubmitWaiter:
    """submit() returns a Wait[Run]-like object; only .run_id is accessed before polling."""
    def __init__(self, run_id):
        self.run_id = run_id


class FakeJobsAPI:
    def __init__(self, final_run: jobs.Run):
        self.submit_calls = []
        self.wait_calls = []
        self._final_run = final_run

    def submit(self, **kwargs):
        self.submit_calls.append(kwargs)
        return _FakeSubmitWaiter(run_id=12345)

    def wait_get_run_job_terminated_or_skipped(self, **kwargs):
        self.wait_calls.append(kwargs)
        return self._final_run


class FakeWorkspaceClient:
    def __init__(self, jobs_api: FakeJobsAPI):
        self.jobs = jobs_api


def _success_run(run_id=12345):
    return jobs.Run(
        run_id=run_id,
        run_page_url="https://x.cloud.databricks.com/jobs/1/runs/1",
        state=jobs.RunState(
            life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
            result_state=jobs.RunResultState.SUCCESS,
            state_message="",
        ),
    )


def _failed_run(run_id=12345, message="notebook raised an exception"):
    return jobs.Run(
        run_id=run_id,
        run_page_url="https://x.cloud.databricks.com/jobs/1/runs/1",
        state=jobs.RunState(
            life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
            result_state=jobs.RunResultState.FAILED,
            state_message=message,
        ),
    )


def _patch_client(monkeypatch, fake_jobs_api):
    monkeypatch.setattr(mod, "_make_workspace_client", lambda h, t: FakeWorkspaceClient(fake_jobs_api))


def _comp(**kw):
    args = dict(asset_name="out", clean_room_name="acme_room", notebook_name="analysis")
    args.update(kw)
    return mod.DatabricksCleanRoomNotebookRunComponent(**args)


def test_successful_run_materializes_with_metadata(monkeypatch):
    fake = FakeJobsAPI(_success_run())
    _patch_client(monkeypatch, fake)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["run_id"].value == 12345
    assert meta["result_state"].value == "SUCCESS"
    assert meta["life_cycle_state"].value == "TERMINATED"

    assert len(fake.submit_calls) == 1
    submitted_task = fake.submit_calls[0]["tasks"][0]
    assert submitted_task.clean_rooms_notebook_task.clean_room_name == "acme_room"
    assert submitted_task.clean_rooms_notebook_task.notebook_name == "analysis"
    assert fake.wait_calls[0]["run_id"] == 12345


def test_failed_run_raises_by_default(monkeypatch):
    fake = FakeJobsAPI(_failed_run(message="boom"))
    _patch_client(monkeypatch, fake)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "boom" in str(failure.event_specific_data.error)


def test_failed_run_does_not_raise_when_disabled(monkeypatch):
    fake = FakeJobsAPI(_failed_run())
    _patch_client(monkeypatch, fake)
    comp = _comp(fail_on_run_failure=False)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["result_state"].value == "FAILED"


def test_etag_and_base_parameters_are_threaded_through(monkeypatch):
    fake = FakeJobsAPI(_success_run())
    _patch_client(monkeypatch, fake)
    comp = _comp(etag="abc123", notebook_base_parameters={"region": "us"})
    dg.materialize(list(comp.build_defs(None).assets))
    task = fake.submit_calls[0]["tasks"][0].clean_rooms_notebook_task
    assert task.etag == "abc123"
    assert task.notebook_base_parameters == {"region": "us"}


def test_run_name_defaults_to_asset_name(monkeypatch):
    fake = FakeJobsAPI(_success_run())
    _patch_client(monkeypatch, fake)
    comp = _comp()
    dg.materialize(list(comp.build_defs(None).assets))
    assert fake.submit_calls[0]["run_name"] == "out"


def test_explicit_run_name_overrides_default(monkeypatch):
    fake = FakeJobsAPI(_success_run())
    _patch_client(monkeypatch, fake)
    comp = _comp(run_name="nightly-clean-room-run")
    dg.materialize(list(comp.build_defs(None).assets))
    assert fake.submit_calls[0]["run_name"] == "nightly-clean-room-run"


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(monkeypatch):
    fake = FakeJobsAPI(_success_run())
    _patch_client(monkeypatch, fake)
    comp = _comp(partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)


def test_missing_host_env_var_raises_a_clear_error(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "DATABRICKS_HOST" in str(failure.event_specific_data.error)
