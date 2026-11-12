"""Committed test for the resource_key retrofit: resource_key set routes the
connection through a registered dagster_snowflake.SnowflakeResource instead of
the inline account_env_var/user_env_var/_connect() path; resource_key unset
leaves the existing _connect() seam (and its env-var defaults) untouched.
"""
import contextlib
import importlib.util
import pathlib
from typing import ClassVar, Optional

import dagster as dg
from dagster_snowflake import SnowflakeResource

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("sofs_resource_key", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SnowflakeOpenflowStatusSensorComponent = _mod.SnowflakeOpenflowStatusSensorComponent


class _FakeCursor:
    def __init__(self, row):
        self._row = row
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, row):
        self._row = row
        self.closed = False
        self.cursor_calls = 0

    def cursor(self):
        self.cursor_calls += 1
        return _FakeCursor(self._row)

    def close(self):
        self.closed = True


class _RecordingSnowflakeResource(SnowflakeResource):
    """A real dagster_snowflake.SnowflakeResource whose get_connection() yields a fake
    connection instead of hitting a real Snowflake account -- same shape as
    dataframe_to_snowflake's resource-path tests, adapted to a sensor (no DataFrame
    write, just a cursor query)."""

    _fake_conn: ClassVar[Optional[_FakeConnection]] = None
    get_connection_calls: ClassVar[int] = 0

    @contextlib.contextmanager
    def get_connection(self, *a, **kw):
        type(self).get_connection_calls += 1
        yield self._fake_conn


def test_resource_key_unset_uses_existing_connect_seam():
    """Unchanged behavior: no resource_key -> required_resource_keys empty,
    _connect() seam still drives the query (exactly test_demo_seam's path)."""
    fake_row = ("2026-11-10T00:00:00Z", 3, "rt-1")

    class _DemoSensor(SnowflakeOpenflowStatusSensorComponent):
        def _connect(self):
            return _FakeConnection(fake_row)

    comp = _DemoSensor(sensor_name="s", flow_name="f", asset_key="snowflake/openflow/f")
    sensor_def = comp.build_defs(None).get_sensor_def("s")
    assert sensor_def.required_resource_keys == set()

    result = sensor_def(dg.build_sensor_context())
    assert result.cursor == "2026-11-10T00:00:00Z"
    assert result.asset_events[0].metadata["openflow_runtime_id"].value == "rt-1"


def test_resource_key_set_routes_through_real_snowflake_resource():
    fake_row = ("2026-11-10T01:00:00Z", 7, "rt-2")
    _RecordingSnowflakeResource._fake_conn = _FakeConnection(fake_row)
    _RecordingSnowflakeResource.get_connection_calls = 0

    comp = SnowflakeOpenflowStatusSensorComponent(
        sensor_name="s2", flow_name="f2", asset_key="snowflake/openflow/f2",
        resource_key="snow",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("s2")
    assert sensor_def.required_resource_keys == {"snow"}

    res = _RecordingSnowflakeResource(account="a", user="u", password="p")
    context = dg.build_sensor_context(resources={"snow": res})
    result = sensor_def(context)

    assert _RecordingSnowflakeResource.get_connection_calls == 1
    assert _RecordingSnowflakeResource._fake_conn.closed is True
    assert result.cursor == "2026-11-10T01:00:00Z"
    assert result.asset_events[0].metadata["openflow_runtime_id"].value == "rt-2"


def test_resource_key_set_with_non_snowflake_resource_skips_with_reason():
    """A resource registered under resource_key but lacking get_connection() (e.g.
    misconfigured) fails loudly as a SensorResult skip, not an unhandled crash."""
    comp = SnowflakeOpenflowStatusSensorComponent(
        sensor_name="s3", flow_name="f3", resource_key="missing",
    )
    sensor_def = comp.build_defs(None).get_sensor_def("s3")
    context = dg.build_sensor_context(resources={"missing": object()})
    result = sensor_def(context)
    assert result.skip_reason is not None
    assert "connect failed" in str(result.skip_reason).lower()
