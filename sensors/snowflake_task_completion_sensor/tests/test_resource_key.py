"""resource_key retrofit for SnowflakeTaskCompletionSensorComponent: when set, the sensor
reads its connection from a registered dagster_snowflake.SnowflakeResource-like resource via
.get_connection() instead of opening its own snowflake.connector.connect(...). When unset,
the existing inline env-var connection path is unchanged.
"""
import contextlib

import dagster as dg
import pytest

from .conftest import FakeConn, load_component_module

mod = load_component_module()
Component = mod.SnowflakeTaskCompletionSensorComponent

ROWS = [
    ("qid_2", "SUCCEEDED", "2024-01-01 01:00:00", "2024-01-01 01:05:00", None, None),
    ("qid_1", "SUCCEEDED", "2024-01-01 00:00:00", "2024-01-01 00:05:00", None, None),
]


def _build(resource_key=None):
    kwargs = dict(
        sensor_name="task_done",
        task_name="MY_TASK",
        database="DB",
        schema_name="SC",
        asset_key="snowflake/tasks/my_task",
        account_env_var="SNOWFLAKE_ACCOUNT",
        user_env_var="SNOWFLAKE_USER",
        password_env_var="SNOWFLAKE_PASSWORD",
    )
    if resource_key:
        kwargs["resource_key"] = resource_key
    return Component(**kwargs)


def test_resource_key_wires_required_resource_keys():
    sensor_def = _build(resource_key="snow").build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == {"snow"}


def test_resource_key_unset_has_no_required_resource_keys():
    sensor_def = _build().build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()


def test_resource_key_set_opens_connection_via_get_connection(monkeypatch):
    import snowflake.connector

    def _boom(**kwargs):
        raise AssertionError("inline snowflake.connector.connect should not be called when resource_key is set")

    monkeypatch.setattr(snowflake.connector, "connect", _boom)

    fake_conn = FakeConn(ROWS)
    calls = {"get_connection": 0}

    class _FakeSnowflakeResource:
        @contextlib.contextmanager
        def get_connection(self):
            calls["get_connection"] += 1
            yield fake_conn

    sensor_def = _build(resource_key="snow").build_defs(None).sensors[0]
    defs = dg.Definitions(sensors=[sensor_def], resources={"snow": _FakeSnowflakeResource()})
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context(resources={"snow": _FakeSnowflakeResource()})
    result = resolved(ctx)

    assert calls["get_connection"] == 1
    assert isinstance(result, dg.SensorResult)
    assert fake_conn.closed is True
    assert len(result.asset_events) == 2
    assert result.cursor == "qid_2"


def test_resource_key_unset_still_uses_inline_env_var_path(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")

    import snowflake.connector

    captured = {}
    fake_conn = FakeConn(ROWS)

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return fake_conn

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)

    sensor_def = _build().build_defs(None).sensors[0]
    defs = dg.Definitions(sensors=[sensor_def])
    resolved = defs.resolve_sensor_def(sensor_def.name)
    ctx = dg.build_sensor_context()
    result = resolved(ctx)

    assert captured["account"] == "acct"
    assert captured["user"] == "user"
    assert captured["password"] == "pw"
    assert isinstance(result, dg.SensorResult)
    assert fake_conn.closed is True
    assert len(result.asset_events) == 2
    assert result.cursor == "qid_2"
