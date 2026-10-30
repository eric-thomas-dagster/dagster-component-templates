"""Real, committed tests for SqlServerStoredProcedureWorkspaceComponent -- a
new component closing a real registry gap: no component wrapped plain T-SQL
stored procedures (not SSIS packages) as Dagster assets, with polling
coverage for runs a SQL Agent job triggered outside Dagster.

Mirrors ssis_workspace's own tested conventions in this registry:
can_subset dependency-ordered execution, assets_by_* overrides, demo_mode
end-to-end via the real write_state_to_path/build_defs_from_state path (not
a hand-rolled state file, which would only prove build_defs_from_state is
demo-safe, not the whole discovery path too).
"""
import asyncio
import json

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
SqlServerStoredProcedureWorkspaceComponent = mod.SqlServerStoredProcedureWorkspaceComponent
SqlServerConnectionConfig = mod.SqlServerConnectionConfig
SqlServerProcedureSelector = mod.SqlServerProcedureSelector


def _component(**overrides):
    attrs = dict(
        connection=SqlServerConnectionConfig(server="sql-01", database="Canvas", user="u", password="p"),
        demo_mode=True,
    )
    attrs.update(overrides)
    return SqlServerStoredProcedureWorkspaceComponent(**attrs)


def _write_state(tmp_path, procedures, server_host="sql-01"):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"server_host": server_host, "procedures": procedures}))
    return p


_TWO_PROCS = [
    {"schema_name": "dbo", "procedure_name": "LoadSilverClaims"},
    {"schema_name": "dbo", "procedure_name": "LoadGoldClaims"},
]


def test_demo_mode_discovers_a_synthetic_catalog(tmp_path):
    comp = _component()
    procs = comp._discover_procedures(None)
    assert len(procs) == 2
    assert {p["procedure_name"] for p in procs} == {"LoadSilverClaims", "LoadGoldClaims"}


def test_demo_mode_write_state_to_path_needs_no_real_engine(tmp_path):
    comp = _component()
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    state = json.loads(state_path.read_text())
    assert len(state["procedures"]) == 2


def test_noop_action_materializes_without_error(tmp_path):
    comp = _component(action="noop")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert len(result.get_asset_materialization_events()) == 2


def test_selecting_one_procedure_executes_only_that_one(tmp_path):
    comp = _component(action="execute")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    silver_key = dg.AssetKey(["sqlserver", "sql-01", "dbo", "LoadSilverClaims"])
    gold_key = dg.AssetKey(["sqlserver", "sql-01", "dbo", "LoadGoldClaims"])

    result = dg.materialize(list(defs.assets), selection=[silver_key], raise_on_error=True)
    materialized = {e.asset_key for e in result.get_asset_materialization_events()}
    assert materialized == {silver_key}
    assert gold_key not in materialized


def test_assets_by_procedure_overrides_key_group_and_deps(tmp_path):
    comp = _component(assets_by_procedure={
        "dbo.LoadSilverClaims": {"key": "silver/claims", "group_name": "silver"},
        "dbo.LoadGoldClaims": {"key": "gold/claims", "deps": ["silver/claims"]},
    })
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    specs_by_key = {s.key: s for s in defs.resolve_all_asset_specs()}
    silver_key = dg.AssetKey(["silver", "claims"])
    gold_key = dg.AssetKey(["gold", "claims"])
    assert specs_by_key[silver_key].group_name == "silver"
    assert dg.AssetKey(["silver", "claims"]) in {d.asset_key for d in specs_by_key[gold_key].deps}


def test_dependency_declared_via_assets_by_procedure_orders_execution(tmp_path):
    comp = _component(
        action="execute",
        assets_by_procedure={
            "dbo.LoadSilverClaims": {"key": "silver/claims"},
            "dbo.LoadGoldClaims": {"key": "gold/claims", "deps": ["silver/claims"]},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    order = [e.asset_key for e in result.get_asset_materialization_events()]
    assert order.index(dg.AssetKey(["silver", "claims"])) < order.index(dg.AssetKey(["gold", "claims"]))


def test_circular_deps_raise_a_clear_error(tmp_path):
    comp = _component(
        action="execute",
        assets_by_procedure={
            "dbo.LoadSilverClaims": {"key": "silver/claims", "deps": ["gold/claims"]},
            "dbo.LoadGoldClaims": {"key": "gold/claims", "deps": ["silver/claims"]},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    with pytest.raises(Exception, match="circular dependency"):
        dg.materialize(list(defs.assets), raise_on_error=True)


def test_build_engine_raises_loudly_if_called_while_demo_mode():
    comp = _component()
    with pytest.raises(RuntimeError, match="demo_mode=true"):
        comp._build_engine()


def test_procedure_selector_filters_by_schema_and_pattern():
    comp = _component(procedure_selector=SqlServerProcedureSelector(include_schemas=["dbo"], include=["Load*"], exclude=["*Gold*"]))
    procs = [
        {"schema_name": "dbo", "procedure_name": "LoadSilverClaims"},
        {"schema_name": "dbo", "procedure_name": "LoadGoldClaims"},
        {"schema_name": "staging", "procedure_name": "LoadStagingClaims"},
        {"schema_name": "dbo", "procedure_name": "SyncRoster"},
    ]
    filtered = comp._apply_selector(procs)
    assert [p["procedure_name"] for p in filtered] == ["LoadSilverClaims"]


# ── Polling sensor: observes SQL-Agent-triggered runs ────────────────────

def test_polling_sensor_skips_procedures_with_no_sql_agent_job_name(tmp_path):
    comp = _component(polling_sensor=True)  # no assets_by_procedure at all
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())
    assert result.asset_events == []
    skip_message = getattr(result.skip_reason, "skip_message", result.skip_reason) or ""
    assert "no procedure" in skip_message


def test_polling_sensor_demo_mode_emits_observations_for_mapped_procedures(tmp_path):
    comp = _component(
        polling_sensor=True,
        assets_by_procedure={
            "dbo.LoadSilverClaims": {"sql_agent_job_name": "Nightly - Load Silver Claims"},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PROCS))
    sensor_def = next(iter(defs.sensors))

    result1 = sensor_def(dg.build_sensor_context())
    assert len(result1.asset_events) == 1
    assert result1.asset_events[0].asset_key == dg.AssetKey(["sqlserver", "sql-01", "dbo", "LoadSilverClaims"])

    # Second tick: a new (different) simulated instance_id -- still only the
    # mapped procedure ever gets events, never the unmapped one.
    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    for e in result2.asset_events:
        assert e.asset_key == dg.AssetKey(["sqlserver", "sql-01", "dbo", "LoadSilverClaims"])
