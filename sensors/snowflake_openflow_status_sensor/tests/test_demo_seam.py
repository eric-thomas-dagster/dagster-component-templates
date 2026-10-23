"""Real, committed test for a gap an external demo-builder hit:
account_env_var/user_env_var were required (no default), and the sensor
called `snowflake.connector.connect(...)` directly inline, so there was no
seam to substitute a fake connection for a demo/test -- a subclass couldn't
skip hitting a real Snowflake account.

Fixed: account_env_var/user_env_var now default to SNOWFLAKE_ACCOUNT/
SNOWFLAKE_USER (so the component can be instantiated without setting any env
vars), and the connect() call is extracted into a single `_connect()` method
a subclass can override entirely, bypassing _connection_kwargs()/_require_env
so no real credentials are ever read.
"""
import importlib.util
import pathlib

import dagster as dg

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("sofs_demo_seam", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SnowflakeOpenflowStatusSensorComponent = _mod.SnowflakeOpenflowStatusSensorComponent


def test_account_and_user_env_var_fields_have_defaults():
    """No kwargs needed beyond sensor_name/flow_name -- this used to raise a
    pydantic validation error for two missing required fields."""
    comp = SnowflakeOpenflowStatusSensorComponent(
        sensor_name="customer_sync_done", flow_name="customer_sync",
    )
    assert comp.account_env_var == "SNOWFLAKE_ACCOUNT"
    assert comp.user_env_var == "SNOWFLAKE_USER"


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, sql):
        pass

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)

    def close(self):
        pass


class _DemoSensor(SnowflakeOpenflowStatusSensorComponent):
    """A demo/test subclass overriding ONLY _connect() -- no real Snowflake
    credentials or env vars ever touched."""

    def _connect(self):
        return _FakeConnection(("2026-10-23T00:00:00Z", 5, "my-runtime"))


def test_subclass_overriding_connect_never_touches_real_env_vars():
    comp = _DemoSensor(
        sensor_name="customer_sync_done",
        flow_name="customer_sync",
        asset_key="snowflake/openflow/customer_sync",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("customer_sync_done")

    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert result.asset_events is not None
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.asset_key == dg.AssetKey(["snowflake", "openflow", "customer_sync"])
    assert event.metadata["openflow_runtime_id"].value == "my-runtime"
    assert result.cursor == "2026-10-23T00:00:00Z"
