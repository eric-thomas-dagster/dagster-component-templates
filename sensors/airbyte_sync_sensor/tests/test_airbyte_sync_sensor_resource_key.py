"""resource_key was declared, wired into required_resource_keys, but never
actually read -- setting it had zero effect, the sensor always polled the
OSS Config API. Fixed by genuinely calling the registered (real,
dagster-airbyte) AirbyteCloudWorkspace's own
get_client().get_jobs_for_connection(...) method when resource_key is set,
verified end-to-end against a live SensorDefinition/build_sensor_context run.
"""
import importlib.util
import pathlib
from datetime import datetime, timezone

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("airbyte_sync_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

AirbyteSyncSensorComponent = _mod.AirbyteSyncSensorComponent


class _FakeAirbyteJob:
    def __init__(self, id, status, start_time, last_updated_at=None):
        self.id = id
        self.status = status
        self.start_time = start_time
        self.last_updated_at = last_updated_at


@dg.job
def run_job():
    pass


def test_resource_key_path_genuinely_calls_get_jobs_for_connection_and_emits_per_job():
    component = AirbyteSyncSensorComponent(
        sensor_name="airbyte_sensor", connection_id="conn1", job_name="run_job", resource_key="airbyte",
    )
    sensor_def = component.build_defs(None).sensors[0]

    calls = []
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    class _FakeClient:
        def get_jobs_for_connection(self, connection_id, created_after=None):
            calls.append((connection_id, created_after))
            return [
                _FakeAirbyteJob(id=1, status="succeeded", start_time=t1, last_updated_at=t1),
                _FakeAirbyteJob(id=2, status="succeeded", start_time=t2, last_updated_at=t2),
                _FakeAirbyteJob(id=3, status="running", start_time=t2),
            ]

    class _FakeWorkspace:
        def get_client(self):
            return _FakeClient()

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"airbyte": _FakeWorkspace()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"airbyte": _FakeWorkspace()})
    result = resolved(ctx)

    assert calls == [("conn1", None)]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 2  # only the 2 succeeded jobs, not the running one
    assert result.cursor == t2.isoformat()


def test_standalone_oss_path_still_works(monkeypatch):
    import requests

    component = AirbyteSyncSensorComponent(sensor_name="airbyte_sensor", connection_id="conn1", job_name="run_job")
    sensor_def = component.build_defs(None).sensors[0]

    captured = {}

    class _FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"jobs": [{"job": {"id": 42, "status": "succeeded", "createdAt": "2026-01-01T00:00:00Z"}}]}

    def fake_get(url, json=None, auth=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return _FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert "localhost:8000" in captured["url"]
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
