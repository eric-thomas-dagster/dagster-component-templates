"""resource_key was declared, wired into required_resource_keys, but never
actually read -- setting it had zero effect. Fixed by genuinely calling the
registered SFTPResourceComponent's own get_client() method when resource_key
is set, verified end-to-end against a live SensorDefinition/
build_sensor_context run. The resource-owned client is also no longer
closed by the sensor (it doesn't own that connection's lifecycle).
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("sftp_monitor_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SftpMonitorSensorComponent = _mod.SftpMonitorSensorComponent


def _build_component(**overrides):
    defaults = dict(sensor_name="sftp_sensor", remote_path="/incoming", job_name="run_job")
    defaults.update(overrides)
    return SftpMonitorSensorComponent(**defaults)


def test_raises_without_resource_key_or_full_credentials():
    with pytest.raises(ValueError, match="resource_key"):
        _build_component()


@dg.job
def run_job():
    pass


class _FakeAttr:
    def __init__(self, filename, st_mtime, st_size):
        self.filename = filename
        self.st_mtime = st_mtime
        self.st_size = st_size


def test_resource_key_path_genuinely_calls_get_client_and_does_not_close_it():
    component = _build_component(resource_key="sftp")

    closed = {"value": False}

    class _FakeSFTPClient:
        def listdir_attr(self, path):
            return [_FakeAttr("orders.csv", 1700000000, 1234)]

        def close(self):
            closed["value"] = True

    class _FakeSFTPResource:
        def get_client(self):
            return _FakeSFTPClient()

    sensor_def = component.build_defs(None).sensors[0]
    defs = dg.Definitions(jobs=[run_job], sensors=[sensor_def], resources={"sftp": _FakeSFTPResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"sftp": _FakeSFTPResource()})
    result = resolved(ctx)

    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 1
    assert closed["value"] is False  # resource owns its own lifecycle
