"""Same bug class as `test_dynamic_table_key_overrides.py`, found in the
`table_modeling: 'virtual'` branch of the tables/views import loop: it built
`AssetSpec(is_virtual=True, key=AssetKey([_table_kwargs["name"]]), ...)`,
unconditionally reading `name` even though `_apply_asset_overrides` pops it
when an override sets `key` -- same `KeyError: 'name'` crash as the dynamic
table case, just in the virtual-table modeling path instead.
"""
import importlib.util
import json
import pathlib

from dagster import AssetKey


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
        import_tables=True,
        table_modeling="virtual",
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


def test_virtual_table_with_key_override_does_not_crash_and_uses_override(tmp_path):
    comp = _component(
        assets_by_name={"ORDERS": {"key": "warehouse/orders"}},
    )
    state_path = _write_state(tmp_path, {
        "tables": [{"TABLE_NAME": "ORDERS", "TABLE_TYPE": "BASE TABLE"}],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["warehouse", "orders"])) is not None


def test_virtual_table_with_no_override_uses_default_key(tmp_path):
    comp = _component()
    state_path = _write_state(tmp_path, {
        "tables": [{"TABLE_NAME": "ORDERS", "TABLE_TYPE": "BASE TABLE"}],
    })
    defs = comp.build_defs_from_state(None, state_path)
    assert defs.get_assets_def(AssetKey(["table_orders"])) is not None
