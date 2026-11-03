"""resource_key was declared, wired into required_resource_keys, but never
actually read -- setting it had zero effect, the sensor always ran its own
inline OAuth + Graph calls. Fixed by genuinely calling the registered
SharePointResourceComponent's own list_library_items(...) method when
resource_key is set, verified end-to-end against a live
SensorDefinition/build_sensor_context run.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("sharepoint_monitor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SharePointMonitorSensorComponent = _mod.SharePointMonitorSensorComponent


def _build_component(**overrides):
    defaults = dict(
        sensor_name="sp_sensor",
        site_url="https://myorg.sharepoint.com/sites/DataTeam",
        library_name="Reports",
        job_name="run_job",
    )
    defaults.update(overrides)
    return SharePointMonitorSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


@dg.job
def run_job():
    pass


def test_resource_key_path_genuinely_calls_list_library_items():
    component = _build_component(resource_key="sp")
    sensor_def = component.build_defs(None).sensors[0]

    calls = []

    class _FakeSharePointResource:
        def list_library_items(self, site_url, library_name, folder_path, top=200):
            calls.append((site_url, library_name, folder_path, top))
            return [{"id": "abc", "name": "report.xlsx", "lastModifiedDateTime": "2026-01-02T00:00:00Z", "webUrl": "http://x", "size": 10}]

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"sp": _FakeSharePointResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"sp": _FakeSharePointResource()})
    result = resolved(ctx)

    assert calls == [("https://myorg.sharepoint.com/sites/DataTeam", "Reports", "", 200)]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.cursor == "2026-01-02T00:00:00Z"


def test_standalone_env_var_path_still_works(monkeypatch):
    import requests

    component = _build_component(
        tenant_id_env_var="AZURE_TENANT_ID",
        client_id_env_var="AZURE_CLIENT_ID",
        client_secret_env_var="AZURE_CLIENT_SECRET",
    )
    sensor_def = component.build_defs(None).sensors[0]
    monkeypatch.setenv("AZURE_TENANT_ID", "t")
    monkeypatch.setenv("AZURE_CLIENT_ID", "c")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "s")

    class _FakeResp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._payload

    def fake_post(url, data=None, timeout=None):
        return _FakeResp({"access_token": "tok"})

    def fake_get(url, headers=None, params=None, timeout=None):
        if "/sites/" in url and "/drives" not in url:
            return _FakeResp({"id": "site1"})
        if url.endswith("/drives"):
            return _FakeResp({"value": [{"id": "drive1", "name": "Reports"}]})
        return _FakeResp({"value": [{"id": "abc", "name": "report.xlsx", "lastModifiedDateTime": "2026-02-02T00:00:00Z", "webUrl": "http://x", "size": 10}]})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
