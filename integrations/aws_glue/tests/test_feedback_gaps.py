"""Tests for the 4 gaps confirmed in component-feedback/2026-11-23-aws-batch-pipes.md item 2:

  1. The observation sensor was keyed by the pre-override `glue_job_<name>`
     asset key, not the FINAL key after `assets_by_job_name`'s override --
     renaming an asset (the override's documented purpose) made the sensor
     report observations for an asset key nothing in the graph has. Fixed by
     keying job_metadata by the resolved AssetSpec.key.
  2. No partitions_def support.
  3. `start_job_run` had no `arguments` template (StartJobRun's real
     `Arguments` boto3 parameter), no {partition_key} substitution.
  4. No optional Pipes support (`use_pipes` + `pipes_bucket` through the
     real, already-upstream `dagster_aws.pipes.PipesGlueClient`).
  5. All jobs shared one @multi_asset -- re-executing one failed job reran
     every selected job. Now one asset (or multi_asset, for 1-job-to-N-assets
     overrides) per job.
  6. `_merge_spec` ignored owners/partitions_def/automation_condition/
     freshness_policy.

Only the boto3 Glue/S3 client boundary is mocked; `dg.materialize()` /
`dg.build_sensor_context()` are real throughout.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


# ── Fake boto3 glue client ──────────────────────────────────────────────────

class _FakeGlueClient:
    def __init__(self):
        self.start_job_run_calls = []
        self._run_id_counter = 0
        self._runs_by_job: dict[str, list] = {}

    def start_job_run(self, JobName, Arguments=None):
        self._run_id_counter += 1
        run_id = f"run-{self._run_id_counter}"
        self.start_job_run_calls.append({"JobName": JobName, "Arguments": Arguments or {}})
        self._runs_by_job.setdefault(JobName, []).append({
            "Id": run_id, "JobRunState": "SUCCEEDED", "StartedOn": None, "CompletedOn": None, "ExecutionTime": 5,
        })
        return {"JobRunId": run_id}

    def get_job_run(self, JobName, RunId):
        for run in self._runs_by_job[JobName]:
            if run["Id"] == RunId:
                return {"JobRun": run}
        raise KeyError(RunId)

    def get_job_runs(self, JobName, MaxResults=5):
        return {"JobRuns": self._runs_by_job.get(JobName, [])}


def _state(**job_overrides):
    job = {
        "name": "etl-job", "description": None, "role": "arn:aws:iam::000:role/x",
        "command_name": "glueetl", "max_capacity": 2.0, "timeout": 60, "tags": {}, "default_arguments": {},
    }
    job.update(job_overrides)
    return {"jobs": [job], "crawlers": [], "workflows": [], "databrew_jobs": [], "data_quality_rulesets": [], "catalog_tables": []}


@pytest.fixture()
def fake_glue(monkeypatch):
    client = _FakeGlueClient()
    monkeypatch.setattr(mod, "_make_glue_client", lambda **kw: client)
    return client


CREDS = dict(region_name="us-east-1", aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)


def _build(fake_glue, **kw):
    defaults = dict(
        state=_state(), group_name="aws_glue", key_prefix=None, poll_interval_seconds=0,
        generate_sensor=True, import_catalog_tables=False, catalog_database_filter=None,
        filter_by_name_pattern=None, exclude_name_pattern=None, filter_by_tags=None,
    )
    defaults.update(CREDS)
    defaults.update(kw)
    return mod._build_glue_defs(**defaults)


def test_jobs_are_per_job_assets_not_one_shared_multi_asset(fake_glue):
    state = _state()
    state["jobs"].append({
        "name": "second-job", "description": None, "role": None, "command_name": "glueetl",
        "max_capacity": 1.0, "timeout": 30, "tags": {}, "default_arguments": {},
    })
    defs = _build(fake_glue, state=state)
    # Two independent executable nodes -- not one @multi_asset spanning both jobs.
    assert len(defs.assets) == 2


def test_materializing_one_job_does_not_start_the_other(fake_glue):
    state = _state()
    state["jobs"].append({
        "name": "second-job", "description": None, "role": None, "command_name": "glueetl",
        "max_capacity": 1.0, "timeout": 30, "tags": {}, "default_arguments": {},
    })
    defs = _build(fake_glue, state=state)
    target = dg.AssetKey(["glue_job_etl_job"])
    result = dg.materialize([a for a in defs.assets], selection=[target], raise_on_error=True)
    assert result.success
    assert len(fake_glue.start_job_run_calls) == 1
    assert fake_glue.start_job_run_calls[0]["JobName"] == "etl-job"


def test_arguments_template_substitutes_partition_key(fake_glue):
    defs = _build(
        fake_glue,
        arguments={"--business_date": "{partition_key}", "--mode": "full"},
        partitions_def=dg.DailyPartitionsDefinition(start_date="2024-01-01"),
    )
    result = dg.materialize(defs.assets, partition_key="2024-01-03", raise_on_error=True)
    assert result.success
    call = fake_glue.start_job_run_calls[0]
    assert call["Arguments"] == {"--business_date": "2024-01-03", "--mode": "full"}


def test_per_job_arguments_override_merges_over_global_template(fake_glue):
    defs = _build(
        fake_glue,
        arguments={"--mode": "full"},
        assets_by_job_name={"etl-job": {"arguments": {"--mode": "incremental"}}},
    )
    result = dg.materialize(defs.assets, raise_on_error=True)
    assert result.success
    assert fake_glue.start_job_run_calls[0]["Arguments"] == {"--mode": "incremental"}


def test_sensor_resolves_overridden_asset_key_not_pre_override_key(fake_glue):
    """The core bug: assets_by_job_name renames the asset, so the sensor must
    report under the RENAMED key, not the independently reconstructed
    glue_job_<name> key."""
    defs = _build(
        fake_glue,
        assets_by_job_name={"etl-job": {"key": "renamed/custom_asset"}},
    )
    fake_glue._runs_by_job["etl-job"] = [
        {"Id": "ext-run-1", "JobRunState": "SUCCEEDED", "StartedOn": None, "CompletedOn": None, "ExecutionTime": 1},
    ]
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context()
    result = sensor_def(ctx)
    asset_keys = {e.asset_key for e in result.asset_events}
    assert dg.AssetKey.from_user_string("renamed/custom_asset") in asset_keys
    assert dg.AssetKey(["glue_job_etl_job"]) not in asset_keys


def test_sensor_handles_one_job_mapping_to_multiple_assets(fake_glue):
    defs = _build(
        fake_glue,
        assets_by_job_name={"etl-job": [{"key": "a/first"}, {"key": "b/second"}]},
    )
    fake_glue._runs_by_job["etl-job"] = [
        {"Id": "ext-run-1", "JobRunState": "SUCCEEDED", "StartedOn": None, "CompletedOn": None, "ExecutionTime": 1},
    ]
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())
    asset_keys = {e.asset_key for e in result.asset_events}
    assert dg.AssetKey.from_user_string("a/first") in asset_keys
    assert dg.AssetKey.from_user_string("b/second") in asset_keys


# ── _merge_spec passthrough ──────────────────────────────────────────────────

def test_merge_spec_passes_through_owners():
    base = dg.AssetSpec(key=dg.AssetKey("x"), owners=["team:base"])
    merged = mod._merge_spec(base, {"owners": ["team:override"]})
    assert merged.owners == ["team:override"]


def test_merge_spec_carries_partitions_def_through():
    pdef = dg.DailyPartitionsDefinition(start_date="2024-01-01")
    base = dg.AssetSpec(key=dg.AssetKey("x"), partitions_def=pdef)
    merged = mod._merge_spec(base, {"description": "renamed"})
    assert merged.partitions_def == pdef


def test_merge_spec_carries_automation_condition_and_freshness_policy_through():
    base = dg.AssetSpec(
        key=dg.AssetKey("x"),
        automation_condition=dg.AutomationCondition.eager(),
        freshness_policy=dg.FreshnessPolicy.time_window(fail_window=__import__("datetime").timedelta(minutes=30)),
    )
    merged = mod._merge_spec(base, {"description": "renamed"})
    assert merged.automation_condition == base.automation_condition
    assert merged.freshness_policy == base.freshness_policy


def test_merge_spec_automation_condition_override_string():
    base = dg.AssetSpec(key=dg.AssetKey("x"))
    merged = mod._merge_spec(base, {"automation_condition": "AutomationCondition.eager()"})
    assert merged.automation_condition == dg.AutomationCondition.eager()


def test_render_arguments_no_partition_key_leaves_placeholder_unset():
    class _Ctx:
        has_partition_key = False
    out = mod._render_arguments({"--date": "{partition_key}"}, _Ctx())
    assert out == {"--date": "{partition_key}"}


# ── use_pipes wiring (PipesGlueClient itself is mocked -- its real behavior
# is dagster_aws's own, already tested upstream; this only tests OUR wiring) ──

def test_use_pipes_calls_pipes_glue_client_with_rendered_arguments(fake_glue, monkeypatch):
    calls = []

    class _FakeInvocation:
        def get_materialize_result(self):
            return dg.MaterializeResult(metadata={"pipes": dg.MetadataValue.text("ok")})

    class _FakePipesGlueClient:
        def __init__(self, client, context_injector, message_reader):
            pass

        def run(self, *, context, start_job_run_params):
            calls.append(start_job_run_params)
            return _FakeInvocation()

    import dagster_aws.pipes as real_pipes
    monkeypatch.setattr(real_pipes, "PipesGlueClient", _FakePipesGlueClient)
    monkeypatch.setattr(real_pipes, "PipesS3ContextInjector", lambda **kw: object())
    monkeypatch.setattr(real_pipes, "PipesS3MessageReader", lambda **kw: object())

    defs = _build(fake_glue, use_pipes=True, pipes_bucket="my-bucket", arguments={"--mode": "full"})
    result = dg.materialize(defs.assets, raise_on_error=True)
    assert result.success
    assert calls == [{"JobName": "etl-job", "Arguments": {"--mode": "full"}}]
    # use_pipes bypasses the raw boto3 start_job_run/get_job_run polling path entirely.
    assert fake_glue.start_job_run_calls == []
