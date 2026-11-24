"""Tests for the 4 gaps confirmed in component-feedback/2026-11-23-aws-batch-pipes.md item 3:

  1. `start_execution(input=json.dumps({}))` was hardcoded -- now an `input_template`
     field with `{partition_key}` substitution, threaded through partitions_def.
  2. The execution's `output` was discarded -- now surfaced as
     `step_functions/output` materialization metadata.
  3. No observation sensor over ListExecutions (skip executions Dagster itself
     started) -- now `generate_sensor: true` adds one, resolving asset keys
     through the SAME `_apply_item_overrides`/`_merge_spec` path the executable
     assets use (not an independently reconstructed key).
  4. `_merge_spec` ignored `owners` -- now passed through.

Only the boto3 `stepfunctions` client boundary is mocked (`_make_boto3_client`);
`dg.materialize()` / `dg.build_sensor_context()` are real throughout.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
StepFunctionsAssetComponent = mod.StepFunctionsAssetComponent


# ── Unit tests: pure helpers ────────────────────────────────────────────────

def test_build_execution_input_defaults_to_empty_object():
    class _Ctx:
        has_partition_key = False
    assert mod._build_execution_input(None, _Ctx()) == "{}"


def test_build_execution_input_substitutes_partition_key():
    class _Ctx:
        has_partition_key = True
        partition_key = "2024-01-03"
    rendered = mod._build_execution_input('{"business_date": "{partition_key}"}', _Ctx())
    assert json.loads(rendered) == {"business_date": "2024-01-03"}


def test_build_execution_input_rejects_malformed_json():
    class _Ctx:
        has_partition_key = False
    with pytest.raises(json.JSONDecodeError):
        mod._build_execution_input("{not valid json", _Ctx())


def test_output_metadata_surfaces_execution_output():
    desc = {"output": json.dumps({"rows_processed": 42})}
    md = mod._output_metadata(desc)
    assert "step_functions/output" in md
    assert md["step_functions/output"].value == {"rows_processed": 42}


def test_output_metadata_empty_when_no_output():
    assert mod._output_metadata({}) == {}


def test_merge_spec_passes_through_owners():
    base = dg.AssetSpec(key=dg.AssetKey("x"), owners=["team:base"])
    merged = mod._merge_spec(base, {"owners": ["team:override"]})
    assert merged.owners == ["team:override"]


def test_merge_spec_keeps_base_owners_when_not_overridden():
    base = dg.AssetSpec(key=dg.AssetKey("x"), owners=["team:base"])
    merged = mod._merge_spec(base, {"description": "new desc"})
    assert merged.owners == ["team:base"]


def test_merge_spec_carries_partitions_def_through_override():
    pdef = dg.DailyPartitionsDefinition(start_date="2024-01-01")
    base = dg.AssetSpec(key=dg.AssetKey("x"), partitions_def=pdef)
    merged = mod._merge_spec(base, {"description": "renamed"})
    assert merged.partitions_def == pdef


# ── Fake boto3 stepfunctions client ─────────────────────────────────────────

class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return self._pages


class _FakeSfnClient:
    def __init__(self):
        self.started = []
        self._executions_by_arn = {}
        self._list_pages = {}

    def start_execution(self, stateMachineArn, name, input):
        self.started.append({"stateMachineArn": stateMachineArn, "name": name, "input": input})
        execution_arn = f"arn:aws:states:us-east-1:000:execution:sm:{name}"
        self._executions_by_arn[execution_arn] = {
            "executionArn": execution_arn,
            "status": "SUCCEEDED",
            "output": json.dumps({"ok": True}),
        }
        return {"executionArn": execution_arn, "startDate": "now"}

    def describe_execution(self, executionArn):
        return self._executions_by_arn[executionArn]

    def get_paginator(self, name):
        assert name == "list_executions"
        return _FakePaginator(self._list_pages.get("list_executions", []))


def _component(fake_client, **kw):
    attrs = dict(region_name="us-east-1")
    attrs.update(kw)
    comp = StepFunctionsAssetComponent(**attrs)
    return comp, fake_client


@pytest.fixture()
def fake_client(monkeypatch):
    client = _FakeSfnClient()
    monkeypatch.setattr(mod, "_make_boto3_client", lambda *a, **k: client)
    return client


MACHINES = [{"name": "my-pipeline", "arn": "arn:aws:states:us-east-1:000:stateMachine:my-pipeline", "type": "STANDARD"}]


def test_execute_sends_dagster_prefixed_name_and_renders_input(fake_client):
    comp, client = _component(
        fake_client, input_template='{"business_date": "{partition_key}"}',
        partition_type="daily", partition_start="2024-01-01",
    )
    defs = mod._build_step_functions_defs(
        machines=MACHINES, region_name="us-east-1", aws_profile=None, role_arn=None,
        group_name="step_functions", key_prefix=None, wait_for_completion=True,
        poll_interval_seconds=0, execution_timeout_seconds=60,
        input_template=comp.input_template, partitions_def=comp._partitions_def,
    )
    result = dg.materialize(defs.assets, partition_key="2024-01-03", raise_on_error=True)
    assert result.success
    assert len(client.started) == 1
    started = client.started[0]
    assert started["name"].startswith(mod.DAGSTER_EXECUTION_NAME_PREFIX)
    assert json.loads(started["input"]) == {"business_date": "2024-01-03"}

    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["step_functions/output"].value == {"ok": True}


def test_sensor_skips_dagster_launched_executions_and_uses_resolved_keys(fake_client):
    client = fake_client
    client._list_pages["list_executions"] = [
        {
            "executions": [
                {"executionArn": "arn:1", "name": f"{mod.DAGSTER_EXECUTION_NAME_PREFIX}abc", "status": "SUCCEEDED", "stopDate": "2024-01-01T00:00:00"},
                {"executionArn": "arn:2", "name": "console-manual-run", "status": "SUCCEEDED", "stopDate": "2024-01-02T00:00:00"},
            ]
        }
    ]
    # assets_by_state_machine_name renames the asset -- the sensor must report
    # under the RENAMED key, not an independently reconstructed one.
    override_key = "renamed/my_pipeline_asset"
    sensor = mod._build_observation_sensor(
        resolved_keys_by_sm_name={"my-pipeline": [dg.AssetKey.from_user_string(override_key)]},
        sm_arn_by_name={"my-pipeline": "arn:aws:states:us-east-1:000:stateMachine:my-pipeline"},
        region_name="us-east-1", aws_profile=None, role_arn=None,
        observation_interval_seconds=60,
    )
    ctx = dg.build_sensor_context()
    result = sensor(ctx)
    assert len(result.asset_events) == 1
    obs = result.asset_events[0]
    assert obs.asset_key == dg.AssetKey.from_user_string(override_key)
    assert obs.metadata["step_functions/execution_name"].value == "console-manual-run"


def test_sensor_resolves_overridden_asset_key_end_to_end(fake_client):
    """The asset builder and the sensor must agree on the final (post-override) key."""
    comp, client = _component(
        fake_client,
        assets_by_state_machine_name={"my-pipeline": {"key": "custom/renamed_asset"}},
        generate_sensor=True,
    )
    defs = mod._build_step_functions_defs(
        machines=MACHINES, region_name="us-east-1", aws_profile=None, role_arn=None,
        group_name="step_functions", key_prefix=None, wait_for_completion=True,
        poll_interval_seconds=0, execution_timeout_seconds=60,
        assets_by_state_machine_name=comp.assets_by_state_machine_name,
        generate_sensor=True,
    )
    assert defs.sensors
    sensor_def = next(iter(defs.sensors))
    asset_graph = defs.resolve_asset_graph()
    assert dg.AssetKey.from_user_string("custom/renamed_asset") in set(
        sensor_def.asset_selection.resolve(asset_graph)
    )
