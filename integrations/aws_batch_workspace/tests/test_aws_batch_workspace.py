"""aws_batch_workspace: real end-to-end tests of the demo-mode seam, enumeration,
assets_by_job_definition overrides, sensor key resolution, and submit_job_params
building.

Only the actual AWS Batch/CloudWatch API calls would be faked in real-mode use
(via `_get_batch_client`/`_get_logs_client`) -- these tests exercise `demo_mode`,
where NOTHING about the real `PipesBatchClient`/`PipesCloudWatchMessageReader`
code is mocked: a real local script opens a real `dagster_pipes.
open_dagster_pipes()` context, and the component's own internal
`_DemoBatchClient`/`_DemoLogsClient` fakes only the boto3 `batch`/`logs` API
surface those real classes call. `dg.materialize()` is real throughout.
"""
import json
import os
import stat
import sys

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
AwsBatchWorkspaceComponent = mod.AwsBatchWorkspaceComponent
BatchJobDefinitionProps = mod.BatchJobDefinitionProps

FAKE_TASK = """#!{python}
import os
from dagster_pipes import open_dagster_pipes
with open_dagster_pipes() as pipes:
    pk = pipes.extras.get("partition_key", "<none>")
    with open(os.environ["FAKE_TASK_LOG"], "a") as f:
        f.write(pk + "\\n")
    pipes.report_asset_materialization(metadata={{"echo": pk}})
"""


@pytest.fixture()
def fake_task(tmp_path):
    script = tmp_path / "fake_task.py"
    script.write_text(FAKE_TASK.format(python=sys.executable))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "task.log"
    os.environ["FAKE_TASK_LOG"] = str(log)
    yield str(script), log
    os.environ.pop("FAKE_TASK_LOG", None)


def _component(**kw):
    attrs = dict(region_name="us-east-1", job_queue="demo-queue")
    attrs.update(kw)
    return AwsBatchWorkspaceComponent(**attrs)


def _state_and_defs(comp, tmp_path):
    state_path = tmp_path / "state.json"
    comp.write_state_to_path(state_path)
    return state_path, comp.build_defs_from_state(None, state_path)


# ── Demo-mode end-to-end (real Pipes protocol) ──────────────────────────────

def test_demo_mode_materializes_via_real_pipes_protocol(fake_task, tmp_path):
    script, log = fake_task
    comp = _component(local_script_path=script)
    _state_path, defs = _state_and_defs(comp, tmp_path)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert log.read_text().splitlines() == ["<none>"]


def test_demo_mode_passes_partition_key(fake_task, tmp_path):
    script, log = fake_task
    comp = _component(local_script_path=script, partition_type="daily", partition_start="2024-01-01")
    _state_path, defs = _state_and_defs(comp, tmp_path)
    result = dg.materialize(list(defs.assets), partition_key="2024-01-03", raise_on_error=True)
    assert result.success
    assert log.read_text().splitlines() == ["2024-01-03"]


def test_single_run_backfill_submits_once_per_partition(fake_task, tmp_path):
    script, log = fake_task
    comp = _component(
        local_script_path=script, backfill_policy="single_run",
        partition_type="daily", partition_start="2024-01-01",
    )
    _state_path, defs = _state_and_defs(comp, tmp_path)
    result = dg.materialize(
        list(defs.assets), raise_on_error=True,
        tags={"dagster/asset_partition_range_start": "2024-01-02", "dagster/asset_partition_range_end": "2024-01-04"},
    )
    assert result.success
    assert log.read_text().splitlines() == ["2024-01-02", "2024-01-03", "2024-01-04"]


def test_demo_mode_requires_local_script_path():
    with pytest.raises(Exception, match="local_script_path"):
        _component(local_script_path=None)


# ── Enumeration + state ──────────────────────────────────────────────────────

def test_write_state_to_path_demo_mode_synthesizes_one_job_definition(tmp_path):
    comp = _component(local_script_path="x.py")
    state_path = tmp_path / "state.json"
    comp.write_state_to_path(state_path)
    state = json.loads(state_path.read_text())
    assert len(state["job_definitions"]) == 1
    assert state["job_definitions"][0]["name"] == "x"


def test_no_state_file_returns_empty_definitions():
    comp = _component(local_script_path="x.py")
    defs = comp.build_defs_from_state(None, None)
    assert not defs.assets


# ── assets_by_job_definition overrides + get_asset_spec ─────────────────────

def test_default_asset_key_is_prefixed_job_definition_name(tmp_path):
    comp = _component(local_script_path="x.py")
    _state_path, defs = _state_and_defs(comp, tmp_path)
    keys = [a.key for a in defs.assets]
    assert dg.AssetKey(["aws_batch", "x"]) in keys


def test_assets_by_job_definition_renames_key(tmp_path):
    comp = _component(
        local_script_path="x.py",
        assets_by_job_definition={"x": {"key": "custom/renamed_batch_job"}},
    )
    _state_path, defs = _state_and_defs(comp, tmp_path)
    keys = [a.key for a in defs.assets]
    assert dg.AssetKey.from_user_string("custom/renamed_batch_job") in keys
    assert dg.AssetKey(["aws_batch", "x"]) not in keys


def test_assets_by_job_definition_merges_owners_and_tags(tmp_path):
    comp = _component(
        local_script_path="x.py",
        assets_by_job_definition={"x": {"owners": ["team:data-eng"], "tags": {"custom": "tag"}}},
    )
    _state_path, defs = _state_and_defs(comp, tmp_path)
    spec = defs.resolve_all_asset_specs()[0]
    assert spec.owners == ["team:data-eng"]
    assert spec.tags.get("custom") == "tag"


def test_translation_callable_overrides_base_spec(tmp_path):
    def _translate(base_spec, props):
        return {"group_name": "custom_group"}

    comp = _component(local_script_path="x.py", translation=_translate)
    _state_path, defs = _state_and_defs(comp, tmp_path)
    spec = defs.resolve_all_asset_specs()[0]
    assert spec.group_name == "custom_group"


def test_get_asset_spec_is_public_hook():
    comp = _component(local_script_path="x.py")
    props = BatchJobDefinitionProps(job_definition_name="myjob", revision=3, job_definition_arn="arn:x")
    spec = comp.get_asset_spec(props)
    assert spec.key == dg.AssetKey(["aws_batch", "myjob"])
    assert spec.metadata["batch/revision"] == 3


# ── submit_job_params building ──────────────────────────────────────────────

def test_build_submit_job_params_basic_shape():
    comp = _component(local_script_path="x.py")
    params = comp._build_submit_job_params("my-job-def", "arn:aws:batch:us-east-1:000:job-definition/my-job-def:1")
    assert params["jobQueue"] == "demo-queue"
    assert params["jobDefinition"] == "arn:aws:batch:us-east-1:000:job-definition/my-job-def:1"
    assert params["jobName"].startswith("my_job_def-")


def test_build_submit_job_params_container_overrides():
    comp = _component(
        local_script_path="x.py", container_command=["python", "run.py"],
        container_environment={"FOO": "bar"}, vcpus_override=2.0, memory_override=4096,
    )
    params = comp._build_submit_job_params("job", "arn")
    override = params["containerOverrides"]
    assert override["command"] == ["python", "run.py"]
    assert override["environment"] == [{"name": "FOO", "value": "bar"}]
    assert override["vcpus"] == 2.0
    assert override["memory"] == 4096


def test_build_submit_job_params_per_job_container_environment_override():
    comp = _component(
        local_script_path="x.py", container_environment={"MODE": "full"},
        assets_by_job_definition={"job": {"container_environment": {"MODE": "incremental"}}},
    )
    params = comp._build_submit_job_params("job", "arn")
    assert params["containerOverrides"]["environment"] == [{"name": "MODE", "value": "incremental"}]


def test_build_submit_job_params_per_job_queue_override():
    comp = _component(
        local_script_path="x.py",
        assets_by_job_definition={"job": {"job_queue": "special-queue"}},
    )
    params = comp._build_submit_job_params("job", "arn")
    assert params["jobQueue"] == "special-queue"


def test_build_submit_job_params_retry_and_timeout():
    comp = _component(local_script_path="x.py", retry_attempts=3, timeout_seconds=600)
    params = comp._build_submit_job_params("job", "arn")
    assert params["retryStrategy"] == {"attempts": 3}
    assert params["timeout"] == {"attemptDurationSeconds": 600}


def test_build_submit_job_params_overrides_deep_merge():
    comp = _component(
        local_script_path="x.py",
        submit_job_params_overrides={"jobQueue": "override-queue", "propagateTags": True},
    )
    params = comp._build_submit_job_params("job", "arn")
    assert params["jobQueue"] == "override-queue"
    assert params["propagateTags"] is True
    assert params["jobDefinition"] == "arn"  # unrelated keys untouched


# ── PipesBatchClient: tags merge dagster/run-id, environment injection ──────

def test_pipes_batch_client_merges_pipes_env_and_dagster_tags(monkeypatch):
    submitted = {}

    class _FakeBatch:
        def submit_job(self, **params):
            submitted.update(params)
            return {"jobId": "job-1", "jobName": params["jobName"]}

        def describe_jobs(self, jobs):
            return {"jobs": [{"jobId": "job-1", "status": "SUCCEEDED", "container": {}}]}

        @property
        def meta(self):
            import types
            return types.SimpleNamespace(region_name="us-east-1")

    class _FakeLogs:
        pass

    from dagster_aws.pipes import PipesCloudWatchMessageReader

    client = mod.PipesBatchClient(client=_FakeBatch(), message_reader=PipesCloudWatchMessageReader(client=_FakeLogs()))

    @dg.asset
    def _asset(context: dg.AssetExecutionContext):
        invocation = client.run(
            context=context,
            submit_job_params={"jobName": "x", "jobQueue": "q", "jobDefinition": "d"},
        )
        return invocation.get_materialize_result()

    result = dg.materialize([_asset], raise_on_error=True)
    assert result.success
    env_names = {e["name"] for e in submitted["containerOverrides"]["environment"]}
    assert any(n.startswith("DAGSTER_PIPES_") for n in env_names)
    assert "dagster/run-id" in submitted["tags"]


# ── Observation sensor: skips Dagster-tagged jobs, resolves overridden key ──

def test_sensor_skips_jobs_with_dagster_run_id_tag(tmp_path):
    class _DescribeJobDefsPaginator:
        def paginate(self, **kwargs):
            return [{
                "jobDefinitions": [
                    {"jobDefinitionName": "x", "revision": 1,
                     "jobDefinitionArn": "arn:aws:batch:us-east-1:000:job-definition/x:1",
                     "containerProperties": {}},
                ]
            }]

    class _ListJobsPaginator:
        def paginate(self, **kwargs):
            return [{
                "jobSummaryList": [
                    {"jobId": "ext-1", "jobName": "manual-run", "status": "SUCCEEDED",
                     "jobDefinition": "arn:aws:batch:us-east-1:000:job-definition/x:1",
                     "tags": {}, "createdAt": 1000},
                    {"jobId": "dagster-1", "jobName": "dagster-run", "status": "SUCCEEDED",
                     "jobDefinition": "arn:aws:batch:us-east-1:000:job-definition/x:1",
                     "tags": {"dagster/run-id": "abc"}, "createdAt": 2000},
                ]
            }]

    class _FakeBatchClient:
        def get_paginator(self, name):
            if name == "describe_job_definitions":
                return _DescribeJobDefsPaginator()
            assert name == "list_jobs"
            return _ListJobsPaginator()

    comp = _component(
        local_script_path="x.py", generate_sensor=True, demo_mode=False,
        assets_by_job_definition={"x": {"key": "renamed/batch_asset"}},
    )
    comp._get_batch_client = lambda: _FakeBatchClient()
    _state_path, defs = _state_and_defs(comp, tmp_path)
    assert defs.sensors
    sensor_def = next(iter(defs.sensors))

    ctx = dg.build_sensor_context()
    result = sensor_def(ctx)
    assert len(result.asset_events) == 1
    obs = result.asset_events[0]
    assert obs.asset_key == dg.AssetKey.from_user_string("renamed/batch_asset")
    assert obs.metadata["batch/job_id"].value == "ext-1"


def test_sensor_skipped_entirely_in_demo_mode(tmp_path):
    comp = _component(local_script_path="x.py", generate_sensor=True)
    _state_path, defs = _state_and_defs(comp, tmp_path)
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())
    assert result.asset_events == []
