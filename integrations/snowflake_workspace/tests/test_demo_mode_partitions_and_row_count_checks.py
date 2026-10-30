"""Real, committed tests for three gaps found reviewing SnowflakeWorkspaceComponent
against a real demo that subclassed it (chpw/snowflake_stored_procedures.py):

1. No demo_mode -- the one network boundary (_create_connection) had no stand-in,
   so the demo built its own. Fixed, scoped to import_stored_procedures (the
   subclass's own scope) with a loud guard against the other 11 import_* flags.
2. No way to verify a stored procedure's output table actually received rows --
   a CALL that "succeeds" can still write zero rows. Fixed via
   procedure_row_count_checks, a subsettable multi_asset_check.
3. No per-entity partitions_def -- the subclass bolted on daily/monthly/multi-axis
   shapes via a hardcoded template_var dispatch. Generalized into a declarative
   `partitions:` key on assets_by_name (any entity, any shape, no subclass).
"""
import asyncio
import importlib.util
import json
import pathlib

import pytest
from dagster import AssetCheckKey, AssetKey, DailyPartitionsDefinition, MultiPartitionsDefinition, build_asset_check_context


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_workspace_component", here / "component.py")
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
        import_stored_procedures=True,
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


def _write_state(tmp_path, state):
    p = tmp_path / "state.json"
    p.write_text(json.dumps(state))
    return p


# ── Gap 1: demo_mode ──────────────────────────────────────────────────────

def test_demo_mode_requires_other_import_flags_disabled(tmp_path):
    comp = _component(demo_mode=True, import_tasks=True)  # import_tasks left on
    with pytest.raises(ValueError, match="demo_mode is currently scoped"):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))


def test_demo_mode_discovers_synthetic_procedures_from_assets_by_name(tmp_path):
    comp = _component(demo_mode=True, assets_by_name={"LOAD_CUSTOMER_METRICS": {}, "LOAD_ORDERS": {}})
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    state = json.loads(state_path.read_text())
    names = {p["NAME"] for p in state["stored_procedures"]}
    assert names == {"LOAD_CUSTOMER_METRICS", "LOAD_ORDERS"}


def test_demo_mode_materializes_procedure_asset_end_to_end(tmp_path):
    import dagster as dg

    comp = _component(demo_mode=True, assets_by_name={"LOAD_CUSTOMER_METRICS": {}})
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success


# ── Gap 2: procedure_row_count_checks ────────────────────────────────────

def test_procedure_row_count_check_passes_in_demo_mode(tmp_path):
    comp = _component(
        demo_mode=True,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {}},
        procedure_row_count_checks=[
            {"procedure": "LOAD_CUSTOMER_METRICS", "table": "ANALYTICS.SILVER.CUSTOMER_METRICS", "blocking": True},
        ],
    )
    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(iter(defs.asset_checks))
    result = next(iter(check(build_asset_check_context())))
    assert result.passed is True
    assert result.metadata["rows_in_output_table"].value > 0


def test_procedure_row_count_check_binds_to_the_overridden_asset_key(tmp_path):
    comp = _component(
        demo_mode=True,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {"key": "silver/customer_metrics"}},
        procedure_row_count_checks=[
            {"procedure": "LOAD_CUSTOMER_METRICS", "table": "ANALYTICS.SILVER.CUSTOMER_METRICS"},
        ],
    )
    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(iter(defs.asset_checks))
    assert check.check_keys == {AssetCheckKey(AssetKey(["silver", "customer_metrics"]), "row_count_nonzero")}


def test_procedure_row_count_check_fails_on_zero_rows_in_real_mode(monkeypatch, tmp_path):
    comp = _component(
        demo_mode=False,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {}},
        procedure_row_count_checks=[
            {"procedure": "LOAD_CUSTOMER_METRICS", "table": "ANALYTICS.SILVER.CUSTOMER_METRICS"},
        ],
    )

    class _FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(comp, "_create_connection", lambda: _FakeConn())
    monkeypatch.setattr(comp, "_execute_query", lambda conn, query: [{"CNT": 0}])

    state_path = _write_state(tmp_path, {})
    defs = comp.build_defs_from_state(None, state_path)
    check = next(iter(defs.asset_checks))
    result = next(iter(check(build_asset_check_context())))
    assert result.passed is False
    assert result.metadata["rows_in_output_table"].value == 0


# ── Gap 3: per-entity partitions via assets_by_name ──────────────────────

def test_assets_by_name_partitions_builds_a_daily_partitions_def(tmp_path):
    comp = _component(
        demo_mode=True,
        assets_by_name={"LOAD_CUSTOMER_METRICS": {"partitions": {"type": "daily", "start": "2024-01-01"}}},
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    assets_def = next(iter(defs.assets))
    assert isinstance(assets_def.partitions_def, DailyPartitionsDefinition)


def test_assets_by_name_partitions_builds_a_multi_axis_partitions_def(tmp_path):
    comp = _component(
        demo_mode=True,
        assets_by_name={
            "LOAD_CUSTOMER_METRICS": {
                "partitions": {
                    "type": "multi",
                    "dimensions": [
                        {"name": "date", "type": "daily", "start": "2024-01-01"},
                        {"name": "region", "type": "static", "values": ["east", "west"]},
                    ],
                }
            }
        },
    )
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    assets_def = next(iter(defs.assets))
    partitions_def = assets_def.partitions_def
    assert isinstance(partitions_def, MultiPartitionsDefinition)
    assert {d.name for d in partitions_def.partitions_defs} == {"date", "region"}


def test_partitions_type_without_start_raises_a_clear_error():
    with pytest.raises(ValueError, match="requires 'start'"):
        mod._build_partitions_def_from_dict({"type": "daily"})
