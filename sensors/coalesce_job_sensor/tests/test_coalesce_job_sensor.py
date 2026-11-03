"""Two real bugs found and fixed here (see component.py docstring for the
full research): the old environment_id+job_id fields assumed a
list-runs-for-job Scheduler API endpoint that doesn't exist (confirmed
against Coalesce's real OpenAPI spec -- the API only has startRun/
runStatus/rerun/cancelRun, none of which list or search runs), and the old
code checked status == "succeeded" when Coalesce's real terminal values
are completed/failed/canceled. Fixed by switching to run_counter (the only
real per-run identifier) and the real status strings. resource_key is also
now genuinely wired to a registered CoalesceResourceComponent's
get_run_status(...).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("coalesce_job_sensor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

CoalesceJobSensorComponent = _mod.CoalesceJobSensorComponent


def _build_component(**overrides):
    defaults = dict(sensor_name="coalesce_sensor", run_counter="12345", job_name="run_job")
    defaults.update(overrides)
    return CoalesceJobSensorComponent(**defaults)


def test_raises_without_resource_key_or_api_token_env_var():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


@dg.job
def run_job():
    pass


def test_resource_key_path_fires_on_real_completed_status():
    component = _build_component(resource_key="coalesce")
    sensor_def = component.build_defs(None).sensors[0]

    class _FakeCoalesceResource:
        def get_run_status(self, run_counter):
            assert run_counter == "12345"
            return "completed"

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"coalesce": _FakeCoalesceResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"coalesce": _FakeCoalesceResource()})
    result = resolved(ctx)

    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert result.cursor == "12345"


def test_never_fires_on_the_fake_succeeded_status_the_old_code_checked_for():
    component = _build_component(resource_key="coalesce")
    sensor_def = component.build_defs(None).sensors[0]

    class _FakeCoalesceResource:
        def get_run_status(self, run_counter):
            return "succeeded"  # not a real Coalesce status value

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"coalesce": _FakeCoalesceResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"coalesce": _FakeCoalesceResource()})
    result = resolved(ctx)

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []


def test_does_not_refire_once_cursor_records_the_run():
    component = _build_component(resource_key="coalesce")
    sensor_def = component.build_defs(None).sensors[0]

    class _FakeCoalesceResource:
        def get_run_status(self, run_counter):
            return "completed"

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"coalesce": _FakeCoalesceResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(cursor="12345", resources={"coalesce": _FakeCoalesceResource()})
    result = resolved(ctx)

    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []


def test_standalone_env_var_path_hits_the_real_run_status_endpoint(monkeypatch):
    import requests

    component = _build_component(api_token_env_var="COALESCE_API_TOKEN")
    sensor_def = component.build_defs(None).sensors[0]
    monkeypatch.setenv("COALESCE_API_TOKEN", "tok")

    captured = {}

    class _FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"runStatus": "completed"}

    def fake_get(url, headers=None, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert captured["url"].endswith("/scheduler/runStatus")
    assert captured["params"] == {"runCounter": "12345"}
    assert captured["headers"]["Authorization"] == "Bearer tok"
    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
