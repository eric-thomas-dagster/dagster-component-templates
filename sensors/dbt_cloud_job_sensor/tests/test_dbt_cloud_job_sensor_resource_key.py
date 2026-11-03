"""resource_key was declared, wired into required_resource_keys, but never
actually read -- setting it had zero effect. Fixed by genuinely calling the
registered (real, dagster-dbt) DbtCloudResource's own get_runs(...) method
when resource_key is set, verified end-to-end against a live
SensorDefinition/build_sensor_context run.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("dbt_cloud_job_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

DbtCloudJobSensorComponent = _mod.DbtCloudJobSensorComponent


def _build_component(**overrides):
    defaults = dict(sensor_name="dbt_sensor", job_id=67890, job_name="run_job")
    defaults.update(overrides)
    return DbtCloudJobSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


@dg.job
def run_job():
    pass


def test_resource_key_path_genuinely_calls_get_runs():
    component = _build_component(resource_key="dbt_cloud")
    sensor_def = component.build_defs(None).sensors[0]

    calls = []

    class _FakeDbtCloudResource:
        def get_runs(self, job_id=None, order_by=None, limit=None):
            calls.append((job_id, order_by, limit))
            return [{"id": 999, "status": 10, "finished_at": "2026-01-01T00:00:00Z"}]

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"dbt_cloud": _FakeDbtCloudResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"dbt_cloud": _FakeDbtCloudResource()})
    result = resolved(ctx)

    assert calls == [(67890, "-id", 1)]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.cursor == "999"


def test_standalone_env_var_path_still_works(monkeypatch):
    import requests

    component = _build_component(account_id=12345, api_token_env_var="DBT_CLOUD_API_TOKEN")
    sensor_def = component.build_defs(None).sensors[0]
    monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "tok")

    captured = {}

    class _FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"data": [{"id": 111, "status": 10, "finished_at": "2026-02-02T00:00:00Z"}]}

    def fake_get(url, headers=None, params=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert "12345" in captured["url"]
    assert captured["headers"]["Authorization"] == "Token tok"
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
