"""Real, committed tests for 3 gaps from the 2026-11-20 Salling/RunMyJobs/Argo/CopyInto
feedback batch (item 4 -- snowflake_workspace):

1. `assets_by_name.<x>.partitions` built DailyPartitionsDefinition(start_date=...) with no
   timezone/end_offset, so it could never be made to line up with a non-UTC date axis used
   elsewhere in the same project. Fixed: timezone/end_offset now accepted on every time-window
   partitions: shape (daily/weekly/monthly/hourly/cron).
2. CALL arguments and EXECUTE TASK ... USING CONFIG payloads were static -- a partitioned
   proc/task recomputed the whole table every run instead of just its partition. Fixed:
   {partition_key} / {partition.<dim>} substitution in `args:` / `config:`.
3. No generic SQL-assertion check beyond procedure_row_count_checks' fixed row-count shape.
   Fixed: `sql_checks: [{asset, name, sql, blocking}]`, one subsettable multi_asset_check.

Mirrors test_demo_mode_partitions_and_row_count_checks.py's style: real write_state_to_path +
build_defs_from_state + dg.materialize()/build_asset_check_context(), monkeypatching only the
Snowflake wire boundary.
"""
import asyncio
import importlib.util
import json
import pathlib

import dagster as dg
import pytest
from dagster import AssetCheckKey, AssetKey, DailyPartitionsDefinition, WeeklyPartitionsDefinition, build_asset_check_context


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "snowflake_workspace_component_tz_args_sqlchecks", here / "component.py"
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
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


# ── Gap 1: timezone / end_offset on partitions: ──────────────────────────

def test_partitions_daily_accepts_timezone_and_end_offset():
    pdef = mod._build_partitions_def_from_dict({
        "type": "daily", "start": "2024-01-01", "timezone": "Europe/Copenhagen", "end_offset": 1,
    })
    assert isinstance(pdef, DailyPartitionsDefinition)
    assert pdef.timezone == "Europe/Copenhagen"
    assert pdef.end_offset == 1


def test_partitions_weekly_accepts_timezone():
    pdef = mod._build_partitions_def_from_dict({
        "type": "weekly", "start": "2024-01-01", "timezone": "America/New_York",
    })
    assert isinstance(pdef, WeeklyPartitionsDefinition)
    assert pdef.timezone == "America/New_York"


def test_partitions_defaults_to_utc_and_zero_end_offset_when_unset():
    pdef = mod._build_partitions_def_from_dict({"type": "daily", "start": "2024-01-01"})
    assert pdef.timezone == "UTC"
    assert pdef.end_offset == 0


def test_assets_by_name_partitions_with_timezone_wires_through_build_defs(tmp_path):
    comp = _component(
        demo_mode=True, import_stored_procedures=True,
        assets_by_name={
            "LOAD_CUSTOMER_METRICS": {
                "partitions": {"type": "daily", "start": "2024-01-01", "timezone": "Europe/Copenhagen"}
            }
        },
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    assets_def = next(iter(defs.assets))
    assert assets_def.partitions_def.timezone == "Europe/Copenhagen"


# ── Gap 2: {partition_key} substitution in task config / proc args ───────

def test_render_partition_placeholders_substitutes_in_strings_dicts_and_lists():
    class _Ctx:
        has_partition_key = True
        partition_key = "2024-06-01"

    assert mod._render_partition_placeholders("{partition_key}", _Ctx()) == "2024-06-01"
    assert mod._render_partition_placeholders(
        {"BUSINESS_DATE": "{partition_key}", "other": 1}, _Ctx(),
    ) == {"BUSINESS_DATE": "2024-06-01", "other": 1}
    assert mod._render_partition_placeholders(["{partition_key}", 2], _Ctx()) == ["2024-06-01", 2]


def test_render_partition_placeholders_is_a_noop_without_a_partition_key():
    class _Ctx:
        has_partition_key = False
        partition_key = None

    assert mod._render_partition_placeholders({"d": "{partition_key}"}, _Ctx()) == {"d": "{partition_key}"}


def test_task_config_substitutes_partition_key_before_execute_task(tmp_path, monkeypatch):
    comp = _component(
        demo_mode=True, import_tasks=True,
        assets_by_name={
            "REFRESH_METRICS": {
                "config": {"BUSINESS_DATE": "{partition_key}"},
                "partitions": {"type": "daily", "start": "2024-01-01"},
            }
        },
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)

    executed = []
    original_execute = mod._DemoSnowflakeCursor.execute

    def recording_execute(self, sql, *args):
        executed.append(" ".join(str(sql).split()))
        return original_execute(self, sql, *args)

    monkeypatch.setattr(mod._DemoSnowflakeCursor, "execute", recording_execute)

    result = dg.materialize(list(defs.assets), partition_key="2024-06-01", raise_on_error=True)
    assert result.success
    execute_task_calls = [q for q in executed if q.upper().startswith("EXECUTE TASK")]
    assert execute_task_calls, "no EXECUTE TASK statement was recorded"
    assert "2024-06-01" in execute_task_calls[0]
    assert "{partition_key}" not in execute_task_calls[0]


def test_proc_args_substitute_partition_key_before_call(tmp_path, monkeypatch):
    comp = _component(
        demo_mode=True, import_stored_procedures=True,
        assets_by_name={
            "LOAD_CUSTOMER_METRICS": {
                "args": ["{partition_key}", "static_value"],
                "partitions": {"type": "daily", "start": "2024-01-01"},
            }
        },
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)

    executed = []
    original_execute = mod._DemoSnowflakeCursor.execute

    def recording_execute(self, sql, *args):
        executed.append(" ".join(str(sql).split()))
        return original_execute(self, sql, *args)

    monkeypatch.setattr(mod._DemoSnowflakeCursor, "execute", recording_execute)

    result = dg.materialize(list(defs.assets), partition_key="2024-06-01", raise_on_error=True)
    assert result.success
    call_statements = [q for q in executed if q.upper().startswith("CALL ")]
    assert call_statements, "no CALL statement was recorded"
    assert "'2024-06-01'" in call_statements[0]
    assert "'static_value'" in call_statements[0]
    assert "{partition_key}" not in call_statements[0]


# ── Gap 3: sql_checks (generic SQL-assertion asset_check) ────────────────

def test_sql_check_passes_in_demo_mode(tmp_path):
    comp = _component(
        demo_mode=True, import_stored_procedures=True,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {}},
        sql_checks=[
            {"asset": "proc_load_customer_metrics", "name": "no_nulls",
             "sql": "SELECT COUNT(*) = 0 AS passed FROM X", "blocking": True},
        ],
    )
    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(c for c in defs.asset_checks if c.node_def.name == "snowflake_sql_checks")
    result = next(iter(check(build_asset_check_context())))
    assert result.passed is True


def test_sql_check_binds_to_a_literal_asset_key(tmp_path):
    comp = _component(
        demo_mode=True,
        sql_checks=[
            {"asset": "warehouse/orders_table", "sql": "SELECT TRUE AS passed"},
        ],
    )
    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(c for c in defs.asset_checks if c.node_def.name == "snowflake_sql_checks")
    assert check.check_keys == {AssetCheckKey(AssetKey(["warehouse", "orders_table"]), "sql_check")}


def test_sql_check_fails_when_query_returns_falsy_passed_in_real_mode(tmp_path, monkeypatch):
    comp = _component(
        demo_mode=False,
        sql_checks=[{"asset": "warehouse/orders_table", "sql": "SELECT COUNT(*) = 0 AS passed FROM X", "blocking": True}],
    )

    class _FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(comp, "_create_connection", lambda: _FakeConn())
    monkeypatch.setattr(comp, "_execute_query", lambda conn, query: [{"PASSED": False}])

    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(c for c in defs.asset_checks if c.node_def.name == "snowflake_sql_checks")
    result = next(iter(check(build_asset_check_context())))
    assert result.passed is False
    from dagster import AssetCheckSeverity
    assert result.severity == AssetCheckSeverity.ERROR


def test_sql_check_and_procedure_row_count_check_coexist(tmp_path):
    comp = _component(
        demo_mode=True, import_stored_procedures=True,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {}},
        procedure_row_count_checks=[
            {"procedure": "LOAD_CUSTOMER_METRICS", "table": "ANALYTICS.SILVER.CUSTOMER_METRICS"},
        ],
        sql_checks=[
            {"asset": "proc_load_customer_metrics", "sql": "SELECT TRUE AS passed"},
        ],
    )
    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    all_check_names = {k.name for check in defs.asset_checks for k in check.check_keys}
    assert "row_count_nonzero" in all_check_names
    assert "sql_check" in all_check_names
