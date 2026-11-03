"""resource_key was declared, wired into required_resource_keys, but never
actually read in the sensor body -- setting it had zero effect, the sensor
always fell back to instance_env_var/username_env_var/password_env_var.
Fixed by genuinely calling the registered ServiceNowResource's own
list_records(...) method when resource_key is set, verified end-to-end
against a live SensorDefinition/build_sensor_context run (not just a
direct function call -- Dagster injects required resources as kwargs
matching the resource key name, so a function that doesn't accept them
would TypeError before the body ever ran; confirmed experimentally).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("servicenow_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ServiceNowSensorComponent = _mod.ServiceNowSensorComponent


def _build_component(**overrides):
    defaults = dict(
        sensor_name="snow_sensor",
        table="change_request",
        sysparm_query="state=approved",
        job_name="run_change",
    )
    defaults.update(overrides)
    return ServiceNowSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


def test_raises_with_partial_inline_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component(instance_env_var="SNOW_INSTANCE")


@dg.job
def run_change():
    pass


def test_resource_key_path_genuinely_calls_list_records():
    component = _build_component(resource_key="snow")
    sensor_def = component.build_defs(None).sensors[0]

    calls = []

    class _FakeServiceNowResource:
        def list_records(self, table, *, query=None, fields=None, limit=100):
            calls.append((table, query, fields, limit))
            return [{"sys_id": "abc123", "sys_updated_on": "2026-01-02 00:00:00", "number": "CHG001", "state": "approved", "short_description": "x"}]

    defs = dg.Definitions(jobs=[run_change], sensors=[sensor_def], resources={"snow": _FakeServiceNowResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"snow": _FakeServiceNowResource()})
    result = resolved(ctx)

    assert len(calls) == 1
    assert calls[0][0] == "change_request"
    assert calls[0][1] == "state=approved"
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.cursor == "2026-01-02 00:00:00"


def test_resource_key_path_skips_when_no_records():
    component = _build_component(resource_key="snow")
    sensor_def = component.build_defs(None).sensors[0]

    class _EmptyResource:
        def list_records(self, table, *, query=None, fields=None, limit=100):
            return []

    defs = dg.Definitions(jobs=[run_change], sensors=[sensor_def], resources={"snow": _EmptyResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"snow": _EmptyResource()})
    result = resolved(ctx)
    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []


def test_standalone_env_var_path_still_works(monkeypatch):
    import os
    import requests

    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "u")
    monkeypatch.setenv("SNOW_PASSWORD", "p")

    component = _build_component(
        instance_env_var="SNOW_INSTANCE",
        username_env_var="SNOW_USERNAME",
        password_env_var="SNOW_PASSWORD",
    )
    sensor_def = component.build_defs(None).sensors[0]

    class _FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"result": [{"sys_id": "x1", "sys_updated_on": "2026-01-01 00:00:00", "number": "CHG002", "state": "approved", "short_description": "y"}]}

    captured = {}

    def fake_get(url, auth=None, params=None, headers=None, timeout=None):
        captured["url"] = url
        captured["auth"] = auth
        return _FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_change], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert captured["auth"] == ("u", "p")
    assert "mycompany.service-now.com" in captured["url"]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
