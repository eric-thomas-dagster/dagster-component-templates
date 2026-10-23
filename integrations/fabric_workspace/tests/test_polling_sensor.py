"""Real, committed tests for a confirmed bug a demo-builder found: `polling_sensor`
(alias `generate_sensor`) is declared as a Field on FabricWorkspaceComponent, with a
docstring promising an AssetObservation-emitting sensor, but `build_defs_from_state`
never read it or built anything -- the flag silently changed nothing
(component-feedback/2026-08-26-fabric-workspace-polling-sensor.md).

Root cause confirmed by reading the whole file: `build_defs_from_state` only ever
calls `self.build_defs_from_state(context, state_path=state_path)`-equivalent asset
construction; there was no sensor-assembly step anywhere, and no
`@dg.sensor`-decorated function in the module at all.

Fixed: `_build_runnable_asset` now tracks each runnable item's FINAL,
translation-resolved AssetKey; `build_defs_from_state` builds one shared polling
sensor (when `polling_sensor=True`) that polls every imported item's real Fabric
`GET .../jobs/instances` job-run history and emits `AssetObservation` for
completions Dagster didn't itself trigger, using the resolved key -- not a
re-derived default -- so sensor/asset identity can't split even under a
`translation:` override (the same class of bug found and fixed in
azure_data_factory and ssis_workspace's own observation sensors).
"""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_polling", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent


def _component(**overrides):
    attrs = dict(
        workspace=_mod.FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=True,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def _one_pipeline_state():
    return {"pipelines": [{"id": "item-1", "displayName": "nightly_ingest", "workspaceId": "ws-1"}]}


def test_polling_sensor_false_by_default_builds_no_sensor(tmp_path):
    comp = _component()
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    assert not list(defs.sensors or [])


def test_polling_sensor_true_builds_a_real_sensor(tmp_path):
    comp = _component(polling_sensor=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    sensors = list(defs.sensors or [])
    assert len(sensors) == 1
    assert sensors[0].name == "fabric_fabric_job_observation_sensor"


def test_sensor_asset_selection_covers_the_resolved_asset_key(tmp_path):
    comp = _component(polling_sensor=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    sensor_def = next(iter(defs.sensors))
    selected = set(sensor_def.asset_selection.selected_keys)
    assert selected == {dg.AssetKey(["fabric_datapipeline_nightly_ingest"])}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_sensor_emits_observation_for_a_new_completed_job_instance(tmp_path, monkeypatch):
    comp = _component(polling_sensor=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    sensor_def = next(iter(defs.sensors))

    monkeypatch.setattr(_mod.FabricResource, "get_token", lambda self: "fake-token")

    import requests
    monkeypatch.setattr(requests, "get", lambda url, headers, timeout: _FakeResponse({
        "value": [{
            "id": "job-instance-1", "itemId": "item-1", "jobType": "Pipeline",
            "invokeType": "Scheduled", "status": "Completed",
            "startTimeUtc": "2026-10-23T00:00:00Z", "endTimeUtc": "2026-10-23T00:01:00Z",
        }]
    }))

    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert isinstance(event, dg.AssetObservation)
    assert event.asset_key == dg.AssetKey(["fabric_datapipeline_nightly_ingest"])
    assert event.metadata["fabric_job_instance_id"].value == "job-instance-1"
    assert json.loads(result.cursor) == {"item-1": "job-instance-1"}


def test_sensor_does_not_reemit_an_already_seen_job_instance(tmp_path, monkeypatch):
    comp = _component(polling_sensor=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    sensor_def = next(iter(defs.sensors))

    monkeypatch.setattr(_mod.FabricResource, "get_token", lambda self: "fake-token")

    import requests
    monkeypatch.setattr(requests, "get", lambda url, headers, timeout: _FakeResponse({
        "value": [{
            "id": "job-instance-1", "itemId": "item-1", "jobType": "Pipeline",
            "invokeType": "Scheduled", "status": "Completed",
            "startTimeUtc": "2026-10-23T00:00:00Z", "endTimeUtc": "2026-10-23T00:01:00Z",
        }]
    }))

    context = dg.build_sensor_context(cursor=json.dumps({"item-1": "job-instance-1"}))
    result = sensor_def(context)

    assert not result.asset_events
    assert result.skip_reason is not None


def test_sensor_respects_translation_override_for_the_resolved_key(tmp_path):
    def _translate(base_asset_spec, props):
        return base_asset_spec._replace(key=dg.AssetKey(["custom", props.object_name]))

    comp = _component(polling_sensor=True, translation=_translate)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _one_pipeline_state()))
    sensor_def = next(iter(defs.sensors))
    selected = set(sensor_def.asset_selection.selected_keys)
    assert selected == {dg.AssetKey(["custom", "nightly_ingest"])}
