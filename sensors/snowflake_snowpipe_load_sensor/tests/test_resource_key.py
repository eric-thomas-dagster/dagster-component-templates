"""Committed test for the resource_key retrofit on SnowflakeSnowpipeLoadSensorComponent.

resource_key set -> required_resource_keys includes it, and the COPY_HISTORY
connection is opened via getattr(context.resources, resource_key).get_connection()
(a real dagster_snowflake.SnowflakeResource whose get_connection() is overridden to
yield a fake connection, instead of hitting real Snowflake).

resource_key unset -> existing inline-field _connect() path is exercised unchanged
(baseline behavior; there were no prior tests for this component).
"""
import contextlib
import datetime
from typing import ClassVar

import dagster as dg
from dagster_snowflake import SnowflakeResource

from .conftest import load_component_module

mod = load_component_module()
SnowflakeSnowpipeLoadSensorComponent = mod.SnowflakeSnowpipeLoadSensorComponent


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.close_calls = 0

    def cursor(self):
        return _FakeCursor(self._rows)

    def close(self):
        self.close_calls += 1


def _row(file_name, when, row_count=10, file_size=1024, status="LOADED", error_count=0, first_error=None):
    return (file_name, when, row_count, file_size, status, error_count, first_error)


def _comp(**overrides):
    kwargs = dict(
        sensor_name="snowpipe_sensor",
        job_name="downstream_job",
        pipe_name="my_pipe",
        destination_table="my_table",
        account="acct",
        user="user",
        database="DB",
        schema="SC",
        password="pw",
    )
    kwargs.update(overrides)
    return SnowflakeSnowpipeLoadSensorComponent(**kwargs)


def test_resource_key_default_none_uses_inline_connect_path():
    rows = [_row("a.csv", datetime.datetime(2024, 1, 1, 12, 0, 0))]
    fake_conn = _FakeConnection(rows)

    class _DemoSensor(SnowflakeSnowpipeLoadSensorComponent):
        def _connect(self):
            return fake_conn

    comp = _DemoSensor(
        sensor_name="snowpipe_sensor", job_name="downstream_job", pipe_name="my_pipe",
        destination_table="my_table", account="acct", user="user", database="DB",
        schema="SC", password="pw",
    )
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()

    result = sensor_def(dg.build_sensor_context())
    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key == "snowpipe:my_pipe:a.csv"
    assert result.cursor == "2024-01-01T12:00:00"
    assert fake_conn.close_calls == 1


def test_resource_key_set_routes_through_real_snowflake_resource():
    rows = [_row("b.csv", datetime.datetime(2024, 2, 1, 9, 30, 0))]
    fake_conn = _FakeConnection(rows)

    class _RecordingSnowflakeResource(SnowflakeResource):
        """A real dagster_snowflake.SnowflakeResource whose get_connection() yields a
        fake connection instead of hitting a real Snowflake account."""

        get_connection_calls: ClassVar[int] = 0

        @contextlib.contextmanager
        def get_connection(self, *a, **kw):
            type(self).get_connection_calls += 1
            yield fake_conn

    comp = _comp(resource_key="snow")
    defs = comp.build_defs(None)
    sensor_def = defs.sensors[0]
    assert sensor_def.required_resource_keys == {"snow"}

    res = _RecordingSnowflakeResource(account="a", user="u", password="p")
    context = dg.build_sensor_context(resources={"snow": res})
    result = sensor_def(context)

    assert _RecordingSnowflakeResource.get_connection_calls == 1
    assert fake_conn.close_calls == 1
    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key == "snowpipe:my_pipe:b.csv"
    assert result.cursor == "2024-02-01T09:30:00"


def test_resource_key_not_registered_raises():
    comp = _comp(resource_key="missing_key")
    sensor_def = comp.build_defs(None).sensors[0]
    context = dg.build_sensor_context()
    try:
        sensor_def(context)
        assert False, "expected an error for an unregistered resource_key"
    except Exception as e:
        assert "missing_key" in str(e) or "resources" in str(e).lower()
