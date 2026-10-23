"""Same bug class as `test_dynamic_table_key_overrides.py`'s dynamic-table
case, found independently by auditing the rest of this file's
`_apply_asset_overrides` call sites: streams/stages/alerts/openflow-flows are
ALL external, AssetSpec-only entity kinds that unconditionally read
`<kwargs>["name"]` to build the AssetSpec's key, crashing with `KeyError:
'name'` whenever `assets_by_name.<NAME>.key` is set (since
`_apply_asset_overrides` pops `name` when an override sets `key`).

These four entity kinds share one `snowflake_observation_sensor` (unlike
dynamic tables' dedicated sensor), whose cursor is JSON -- so the fix keys
`stream_metadata`/`stage_metadata`/`alert_metadata`/`openflow_metadata` by the
resolved key's `to_user_string()` (a plain string, JSON-safe) rather than the
AssetKey object itself, and reconstructs the real AssetKey at emission time
via `AssetKey.from_user_string(...)` instead of the old `AssetKey([name])`
(which would silently mis-handle a multi-segment override key).
"""
import importlib.util
import json
import pathlib

from dagster import AssetKey, SensorEvaluationContext, build_sensor_context


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "snowflake_workspace_component", here / "component.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
SnowflakeWorkspaceComponent = mod.SnowflakeWorkspaceComponent


def _component(**overrides):
    from dagster_snowflake import SnowflakeResource

    attrs = dict(
        workspace=SnowflakeResource(
            account="fake-account", user="fake-user", password="fake-password",
            warehouse="fake-wh", database="FAKE_DB", schema="FAKE_SCHEMA",
        ),
        import_tasks=False,
        import_snowpipes=False,
        import_dynamic_tables=False,
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def test_stream_with_key_override_does_not_crash_and_uses_override(tmp_path):
    comp = _component(
        import_streams=True,
        assets_by_name={"ORDERS_STREAM": {"key": "warehouse/orders_stream"}},
    )
    state_path = _write_state(tmp_path, {
        "streams": [{
            "NAME": "ORDERS_STREAM", "DATABASE_NAME": "FAKE_DB",
            "SCHEMA_NAME": "FAKE_SCHEMA", "TABLE_NAME": "ORDERS", "TYPE": "DELTA",
        }],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "orders_stream"])) is not None


def test_stage_with_key_override_does_not_crash_and_uses_override(tmp_path):
    comp = _component(
        import_stages=True,
        assets_by_name={"RAW_STAGE": {"key": "warehouse/raw_stage"}},
    )
    state_path = _write_state(tmp_path, {
        "stages": [{
            "NAME": "RAW_STAGE", "DATABASE_NAME": "FAKE_DB",
            "SCHEMA_NAME": "FAKE_SCHEMA", "URL": "s3://bucket/raw", "TYPE": "EXTERNAL",
        }],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "raw_stage"])) is not None


def test_alert_with_key_override_does_not_crash_and_uses_override(tmp_path):
    comp = _component(
        import_alerts=True,
        assets_by_name={"HIGH_VALUE_ALERT": {"key": "warehouse/high_value_alert"}},
    )
    state_path = _write_state(tmp_path, {
        "alerts": [{
            "NAME": "HIGH_VALUE_ALERT", "DATABASE_NAME": "FAKE_DB",
            "SCHEMA_NAME": "FAKE_SCHEMA", "CONDITION": "SELECT 1",
        }],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "high_value_alert"])) is not None


def test_openflow_flow_with_key_override_does_not_crash_and_uses_override(tmp_path):
    comp = _component(
        import_openflow_flows=True,
        assets_by_name={"INGEST_FLOW": {"key": "warehouse/ingest_flow"}},
    )
    state_path = _write_state(tmp_path, {
        "openflow_flows": [{"FLOW_NAME": "INGEST_FLOW", "RUNTIME_ID": "rt-1"}],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "ingest_flow"])) is not None


class _FakeCursor:
    """Stands in for a real Snowflake DB-API cursor: dispatches canned
    results by matching a distinctive substring in the SQL text, which is
    all the stream-observation probe code path needs."""

    def __init__(self):
        self._last_sql = ""

    def execute(self, sql, *a, **k):
        self._last_sql = sql

    def fetchone(self):
        if "SYSTEM$STREAM_HAS_DATA" in self._last_sql:
            return ("TRUE",)
        if "QUERY_HISTORY" in self._last_sql:
            return (3, "2026-01-01T00:00:00Z")
        if "COUNT(*)" in self._last_sql:
            return (3,)
        return (None,)

    def close(self):
        pass


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()

    def close(self):
        pass


def test_observation_sensor_cursor_and_emission_use_the_overridden_multi_segment_key(tmp_path, monkeypatch):
    """End-to-end: run the shared observation sensor once against a stream with
    a multi-segment override key, and confirm both the emitted
    AssetMaterialization's key AND the persisted JSON cursor correctly reflect
    the real, multi-segment key (not a single-segment AssetKey([...]) that
    would have silently clobbered it)."""
    comp = _component(
        import_streams=True,
        assets_by_name={"ORDERS_STREAM": {"key": "warehouse/orders_stream"}},
    )
    state_path = _write_state(tmp_path, {
        "streams": [{
            "NAME": "ORDERS_STREAM", "DATABASE_NAME": "FAKE_DB",
            "SCHEMA_NAME": "FAKE_SCHEMA", "TABLE_NAME": "ORDERS", "TYPE": "DELTA",
        }],
    })
    defs = comp.build_defs_from_state(None, state_path)
    sensor_def = defs.get_sensor_def(f"{comp.group_name}_observation_sensor")

    monkeypatch.setattr(comp, "_create_connection", lambda: _FakeConnection())

    context: SensorEvaluationContext = build_sensor_context()
    result = sensor_def(context)

    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.asset_key == AssetKey(["warehouse", "orders_stream"])

    cursor = json.loads(result.cursor)
    assert "warehouse/orders_stream" in cursor["streams"]
