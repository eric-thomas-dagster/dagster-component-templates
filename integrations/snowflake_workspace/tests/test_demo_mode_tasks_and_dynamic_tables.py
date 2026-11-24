"""Real, committed tests for extending demo_mode past stored procedures.

Before this fix, demo_mode's synthetic Snowflake connection
(`_DemoSnowflakeCursor`/`_DemoSnowflakeConnection`) only answered the SQL
shapes the stored-procedure discovery + CALL path sends (SHOW PROCEDURES,
CALL <proc>(...)), and write_state_to_path RAISED a clear error if
import_tasks or import_dynamic_tables was also enabled alongside demo_mode
-- so you couldn't smoke-test the (default-on!) task import path, or
dynamic tables, with no real Snowflake account.

Fixed: the demo connection now also answers:
  - SHOW TASKS [LIKE '<name>'] IN SCHEMA ... (discovery + per-task
    enrichment in _run_task_body)
  - EXECUTE TASK ... [USING CONFIG = '<json>']
  - the TASK_HISTORY table function
  - SHOW DYNAMIC TABLES [LIKE '<name>'] IN SCHEMA ... (discovery, the
    snowflake_dt_refresh_sensor, and per-DT enrichment in
    _make_dynamic_table_asset)
  - ALTER DYNAMIC TABLE ... REFRESH
  - the DYNAMIC_TABLE_REFRESH_HISTORY table function

...and the guard in write_state_to_path no longer blocks import_tasks /
import_dynamic_tables -- only the entity kinds still NOT covered (streams,
snowpipes, stages, materialized views, external tables, alerts, OpenFlow
flows, tables, views; see test_demo_mode_requires_other_import_flags_disabled
in test_demo_mode_partitions_and_row_count_checks.py).

Mirrors that file's existing demo_mode-for-stored-procedures tests: runs
write_state_to_path for real (async), then build_defs_from_state + a real
dg.materialize() -- no monkeypatching, no real Snowflake connection.
"""
import asyncio
import importlib.util
import json
import pathlib

import dagster as dg


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "snowflake_workspace_component_demo_tasks_dts", here / "component.py"
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
        import_stored_procedures=False,
        import_dynamic_tables=False,
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


# ── demo_mode + tasks ────────────────────────────────────────────────────

def test_demo_mode_discovers_synthetic_tasks_from_assets_by_name(tmp_path):
    comp = _component(
        demo_mode=True, import_tasks=True,
        assets_by_name={"REFRESH_METRICS": {}, "LOAD_ORDERS": {}},
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    state = json.loads(state_path.read_text())
    names = {t["NAME"] for t in state["tasks"]}
    assert names == {"REFRESH_METRICS", "LOAD_ORDERS"}
    # Discovery rows carry DATABASE_NAME/SCHEMA_NAME -- the import loop reads
    # these with direct bracket access (task['DATABASE_NAME']), not .get(),
    # so a missing column is a real KeyError crash, not a soft failure.
    for t in state["tasks"]:
        assert t["DATABASE_NAME"] == "FAKE_DB"
        assert t["SCHEMA_NAME"] == "FAKE_SCHEMA"


def test_demo_mode_materializes_task_asset_end_to_end(tmp_path):
    """Exercises EXECUTE TASK, the SHOW TASKS LIKE per-task enrichment, and
    the TASK_HISTORY table function -- all three must not raise."""
    comp = _component(demo_mode=True, import_tasks=True, assets_by_name={"REFRESH_METRICS": {}})
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    event = next(
        e for e in result.get_asset_materialization_events()
        if e.asset_key == dg.AssetKey("task_refresh_metrics")
    )
    metadata = event.materialization.metadata
    assert metadata["task_name"].value == "REFRESH_METRICS"
    # TASK_HISTORY enrichment landed (state/return_value from the synthetic row).
    assert metadata["return_value"].value == "Demo run completed successfully"


# ── demo_mode + dynamic tables ───────────────────────────────────────────

def test_demo_mode_discovers_synthetic_dynamic_tables_from_assets_by_name(tmp_path):
    comp = _component(
        demo_mode=True, import_dynamic_tables=True,
        assets_by_name={"CUSTOMER_360_DT": {}},
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    state = json.loads(state_path.read_text())
    names = {d["NAME"] for d in state["dynamic_tables"]}
    assert names == {"CUSTOMER_360_DT"}
    for d in state["dynamic_tables"]:
        assert d["DATABASE_NAME"] == "FAKE_DB"
        assert d["SCHEMA_NAME"] == "FAKE_SCHEMA"


def test_demo_mode_materializes_dynamic_table_asset_end_to_end(tmp_path):
    """dt_modeling='asset' (legacy, manual-refresh path) exercises
    ALTER DYNAMIC TABLE ... REFRESH + the SHOW DYNAMIC TABLES LIKE per-DT
    enrichment -- both must not raise."""
    comp = _component(
        demo_mode=True, import_dynamic_tables=True, dt_modeling="asset",
        assets_by_name={"CUSTOMER_360_DT": {}},
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    # The legacy DT asset function returns its metadata dict as the op's
    # plain output value (not wrapped in a MaterializeResult), so this is
    # the real way to see that ALTER DYNAMIC TABLE ... REFRESH and the
    # SHOW DYNAMIC TABLES LIKE enrichment both actually ran.
    output = result.output_for_node("dynamic_table_customer_360_dt")
    assert output["table_name"] == "CUSTOMER_360_DT"
    assert output["refresh_mode"] == "INCREMENTAL"


def test_demo_mode_dynamic_table_external_mode_declares_asset_spec_with_no_crash(tmp_path):
    """dt_modeling='external' (the default) is AssetSpec-only -- no compute,
    so there's nothing to materialize, but discovery + build_defs_from_state
    must still not crash and must carry the DT's target_lag/refresh_mode."""
    comp = _component(
        demo_mode=True, import_dynamic_tables=True,
        assets_by_name={"CUSTOMER_360_DT": {}},
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    spec = next(iter(defs.assets))
    assert spec.key == dg.AssetKey("dynamic_table_customer_360_dt")
    assert spec.metadata["snowflake_target_lag"] == "1 minute"


# ── demo_mode with stored procedures + tasks + dynamic tables combined ──

def test_demo_mode_combines_procedures_tasks_and_dynamic_tables_with_no_guard_error(tmp_path):
    """The actual gap: this used to raise 'demo_mode is currently scoped to
    import_stored_procedures only' the moment a second import_* flag turned
    on. Now all three run together from one component instance, each with
    its own distinct assets_by_name entry (default per-kind key prefixes --
    proc_/task_/dynamic_table_ -- keep them from colliding even though they
    all come from the same assets_by_name dict)."""
    comp = _component(
        demo_mode=True,
        import_stored_procedures=True,
        import_tasks=True,
        import_dynamic_tables=True,
        dt_modeling="asset",
        assets_by_name={
            "LOAD_CUSTOMER_METRICS": {},
            "REFRESH_METRICS_TASK": {},
            "CUSTOMER_360_DT": {},
        },
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    state = json.loads(state_path.read_text())
    assert {p["NAME"] for p in state["stored_procedures"]} == {
        "LOAD_CUSTOMER_METRICS", "REFRESH_METRICS_TASK", "CUSTOMER_360_DT",
    }
    assert {t["NAME"] for t in state["tasks"]} == {
        "LOAD_CUSTOMER_METRICS", "REFRESH_METRICS_TASK", "CUSTOMER_360_DT",
    }
    assert {d["NAME"] for d in state["dynamic_tables"]} == {
        "LOAD_CUSTOMER_METRICS", "REFRESH_METRICS_TASK", "CUSTOMER_360_DT",
    }

    defs = comp.build_defs_from_state(None, state_path)
    asset_keys = {a.key for a in defs.assets}
    # 3 names x 3 kinds = 9 distinct default-prefixed asset keys, no collisions.
    assert len(asset_keys) == 9

    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
