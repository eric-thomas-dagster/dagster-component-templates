"""Real, committed tests for a bug an external demo-builder hit: `dt_modeling:
'external'` + `assets_by_name.<NAME>.key` crashed with `KeyError: 'name'`.

Root cause: `_apply_asset_overrides` pops `name` when an override sets `key`
(so `@asset`/`AssetSpec` don't get both), but the dynamic-table import loop's
`external` branch unconditionally read `_dt_ext_kwargs["name"]` to build the
AssetSpec's key, ignoring the override's `key` entirely. The legacy
(`dt_modeling: "asset"`) branch didn't crash (`@asset(**kwargs)` accepts either
`name=` or `key=`), but shared a quieter sibling bug: the DT-refresh sensor's
`dt_asset_keys`/`dt_name_to_asset_key` were populated with the *default*,
pre-override `dynamic_table_<name>` key in BOTH branches, so the sensor would
emit materializations/selections against the wrong asset once an override (or
a `translation:` callable) changed the key.

Auditing the other ~11 call sites of `_apply_asset_overrides` in this file
turned up the SAME bug, independently, in streams/stages/alerts/openflow-flows
(all `AssetSpec`-only, external/observable entity kinds) and the
`table_modeling: "virtual"` / `view_modeling: "virtual"` table path -- same
unconditional `kwargs["name"]` read building the AssetSpec's key. Those are
covered in `test_stream_stage_alert_flow_key_overrides.py` and
`test_virtual_table_key_override.py`; fixed the same way (resolve `key` from
the override first, fall back to `name` only when no override set one).

This exercises `build_defs_from_state` directly against a hand-written state
JSON (the shape `write_state_to_path` would have produced) -- no real
Snowflake connection needed, since state-backed `build_defs_from_state` does
zero Snowflake I/O at load time.
"""
import importlib.util
import json
import pathlib

import pytest
from dagster import AssetKey, AssetSpec


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
        import_dynamic_tables=True,
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


def _state_with_one_dt(name="ORDERS_DT"):
    return {
        "dynamic_tables": [
            {
                "NAME": name,
                "DATABASE_NAME": "FAKE_DB",
                "SCHEMA_NAME": "FAKE_SCHEMA",
                "TARGET_LAG": "1 hour",
                "REFRESH_MODE": "INCREMENTAL",
            }
        ],
    }


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def _spec_for(defs, key: AssetKey):
    return defs.get_assets_def(key).get_asset_spec(key)


# --- external modeling (AssetSpec, declare-only) ---------------------------

def test_external_dt_with_no_override_uses_default_key(tmp_path):
    comp = _component(dt_modeling="external")
    state_path = _write_state(tmp_path, _state_with_one_dt())
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["dynamic_table_orders_dt"])) is not None


def test_external_dt_with_key_override_does_not_crash_and_uses_override(tmp_path):
    """This is the exact crash the external feedback reported: KeyError: 'name'."""
    comp = _component(
        dt_modeling="external",
        assets_by_name={"ORDERS_DT": {"key": "warehouse/orders_dt"}},
    )
    state_path = _write_state(tmp_path, _state_with_one_dt())
    defs = comp.build_defs_from_state(None, state_path)
    override_key = AssetKey(["warehouse", "orders_dt"])
    assert defs.get_assets_def(override_key) is not None
    # The default, pre-override key must NOT also exist.
    with pytest.raises(Exception):
        defs.get_assets_def(AssetKey(["dynamic_table_orders_dt"]))


def test_external_dt_sensor_asset_selection_uses_the_overridden_key(tmp_path):
    comp = _component(
        dt_modeling="external",
        assets_by_name={"ORDERS_DT": {"key": "warehouse/orders_dt"}},
    )
    state_path = _write_state(tmp_path, _state_with_one_dt())
    defs = comp.build_defs_from_state(None, state_path)
    sensor = defs.get_sensor_def("snowflake_dt_refresh_sensor")
    selected_keys = set(sensor.asset_selection.selected_keys)
    assert AssetKey(["warehouse", "orders_dt"]) in selected_keys
    assert AssetKey(["dynamic_table_orders_dt"]) not in selected_keys


# --- legacy modeling (@asset, manual REFRESH) ------------------------------

def test_legacy_dt_with_key_override_builds_with_overridden_key(tmp_path):
    comp = _component(
        dt_modeling="asset",
        assets_by_name={"ORDERS_DT": {"key": "warehouse/orders_dt"}},
    )
    state_path = _write_state(tmp_path, _state_with_one_dt())
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "orders_dt"])) is not None


def test_legacy_dt_sensor_asset_selection_uses_the_overridden_key(tmp_path):
    """Pre-fix, this sensor would have emitted/selected against
    dynamic_table_orders_dt (the pre-override default) even though the
    legacy @asset itself was correctly keyed -- a silent, no-crash bug."""
    comp = _component(
        dt_modeling="asset",
        assets_by_name={"ORDERS_DT": {"key": "warehouse/orders_dt"}},
    )
    state_path = _write_state(tmp_path, _state_with_one_dt())
    defs = comp.build_defs_from_state(None, state_path)
    sensor = defs.get_sensor_def("snowflake_dt_refresh_sensor")
    selected_keys = set(sensor.asset_selection.selected_keys)
    assert AssetKey(["warehouse", "orders_dt"]) in selected_keys
    assert AssetKey(["dynamic_table_orders_dt"]) not in selected_keys


# --- multiple DTs: overrides are per-entity, not all-or-nothing ------------

def test_mixed_overrides_only_affect_the_named_entity(tmp_path):
    comp = _component(
        dt_modeling="external",
        assets_by_name={"ORDERS_DT": {"key": "warehouse/orders_dt"}},
    )
    state = _state_with_one_dt("ORDERS_DT")
    state["dynamic_tables"].append({
        "NAME": "SHIPMENTS_DT",
        "DATABASE_NAME": "FAKE_DB",
        "SCHEMA_NAME": "FAKE_SCHEMA",
        "TARGET_LAG": "1 hour",
        "REFRESH_MODE": "INCREMENTAL",
    })
    state_path = _write_state(tmp_path, state)
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "orders_dt"])) is not None
    assert defs.get_assets_def(AssetKey(["dynamic_table_shipments_dt"])) is not None
