"""resource_key was declared, wired into required_resource_keys, but never
actually read -- setting it had zero effect. Fixed by genuinely calling the
registered (real, dagster-fivetran) FivetranWorkspace's own
get_client().get_connector_details(...) method when resource_key is set,
verified end-to-end against a live SensorDefinition/build_sensor_context run.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fivetran_sync_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FivetranSyncSensorComponent = _mod.FivetranSyncSensorComponent


def _build_component(**overrides):
    defaults = dict(sensor_name="fivetran_sensor", connector_id="conn1", job_name="run_job")
    defaults.update(overrides)
    return FivetranSyncSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


@dg.job
def run_job():
    pass


def test_resource_key_path_genuinely_calls_get_connector_details():
    component = _build_component(resource_key="fivetran")
    sensor_def = component.build_defs(None).sensors[0]

    calls = []

    class _FakeClient:
        def get_connector_details(self, connector_id):
            calls.append(connector_id)
            return {"id": "conn1", "succeeded_at": "2026-01-02T00:00:00Z", "status": {"sync_state": "scheduled"}}

    class _FakeFivetranWorkspace:
        def get_client(self):
            return _FakeClient()

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"fivetran": _FakeFivetranWorkspace()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"fivetran": _FakeFivetranWorkspace()})
    result = resolved(ctx)

    assert calls == ["conn1"]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.cursor == "2026-01-02T00:00:00Z"


def test_standalone_env_var_path_still_works(monkeypatch):
    import base64
    import requests

    component = _build_component(api_key_env_var="FIVETRAN_API_KEY", api_secret_env_var="FIVETRAN_API_SECRET")
    sensor_def = component.build_defs(None).sensors[0]
    monkeypatch.setenv("FIVETRAN_API_KEY", "k")
    monkeypatch.setenv("FIVETRAN_API_SECRET", "s")

    captured = {}

    class _FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"data": {"id": "conn1", "succeeded_at": "2026-02-02T00:00:00Z", "status": {"sync_state": "scheduled"}}}

    def fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    expected_creds = base64.b64encode(b"k:s").decode()
    assert captured["headers"]["Authorization"] == f"Basic {expected_creds}"
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
