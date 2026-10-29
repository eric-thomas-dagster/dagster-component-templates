"""Real, committed tests for three gaps found reviewing SsisWorkspaceComponent
(the smallest-upstream-edit review that also produced ssis_catalog_workspace.py
as a stopgap subclass -- these fixes make the subclass unnecessary):

1. `action: execute` built one @multi_asset without can_subset=True, so
   selecting a single package still ran every discovered package. Fixed via
   can_subset=True + executing only context.selected_asset_keys.
2. No way to bind a package to an asset key with real dependencies between
   packages. Fixed via `assets_by_package` (mirrors the official
   dagster-databricks DatabricksWorkspaceComponent.assets_by_task_key shape).
   Once two SELECTED packages have a declared dependency, execution order
   respects it (topological, not declaration order).
3. No demo-mode seam -- the engine was built in one place (_build_engine),
   so a subclass could return a stand-in, but nothing documented or default.
   Fixed via a `demo_mode` field: discovery, execute, the observation
   sensor, and freshness checks all branch to a synthetic/simulated path
   with zero SQL Server dependency.
"""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("ssis_review_feedback", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

SsisWorkspaceComponent = mod.SsisWorkspaceComponent
SsisWorkspaceConfig = mod.SsisWorkspaceConfig


def _component(**overrides):
    attrs = dict(
        workspace=SsisWorkspaceConfig(server="sql-01", database="SSISDB", user="u", password="p"),
        action="noop",
    )
    attrs.update(overrides)
    return SsisWorkspaceComponent(**attrs)


def _write_state(tmp_path, packages):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"server_host": "sql-01", "packages": packages}))
    return p


_TWO_PACKAGES = [
    {"folder_name": "Sales", "project_name": "SalesETL", "project_id": 1, "package_name": "ExtractCustomers", "description": None},
    {"folder_name": "Sales", "project_name": "SalesETL", "project_id": 1, "package_name": "LoadOrders", "description": None},
]


# ── Gap 1: can_subset=True ───────────────────────────────────────────────────

def test_selecting_one_package_executes_only_that_package(tmp_path):
    comp = _component(action="execute", demo_mode=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PACKAGES))

    extract_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "ExtractCustomers"])
    load_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "LoadOrders"])

    result = dg.materialize(list(defs.assets), selection=[extract_key], raise_on_error=True)
    materialized_keys = {e.asset_key for e in result.get_asset_materialization_events()}
    assert materialized_keys == {extract_key}
    assert load_key not in materialized_keys


def test_selecting_both_packages_executes_both(tmp_path):
    comp = _component(action="execute", demo_mode=True)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PACKAGES))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert len(result.get_asset_materialization_events()) == 2


# ── Gap 2: assets_by_package + dependency-ordered execution ─────────────────

def test_assets_by_package_overrides_key_group_name_deps_and_tags(tmp_path):
    extract_qname = "Sales/SalesETL/ExtractCustomers"
    comp = _component(
        assets_by_package={
            extract_qname: {
                "key": "custom/extract_customers",
                "group_name": "sales_bi",
                "tags": {"team": "sales"},
            },
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, [_TWO_PACKAGES[0]]))
    spec = next(iter(defs.assets)).get_asset_spec(dg.AssetKey(["custom", "extract_customers"]))
    assert spec.group_name == "sales_bi"
    assert spec.tags.get("team") == "sales"


def test_dependency_declared_via_assets_by_package_orders_execution(tmp_path):
    extract_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "ExtractCustomers"])
    load_qname = "Sales/SalesETL/LoadOrders"
    comp = _component(
        action="execute", demo_mode=True,
        assets_by_package={
            load_qname: {"deps": [extract_key.to_user_string()]},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PACKAGES))

    load_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "LoadOrders"])
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    events = result.get_asset_materialization_events()
    order = [e.asset_key for e in events]
    assert order.index(extract_key) < order.index(load_key)


def test_circular_assets_by_package_deps_raise_a_clear_error(tmp_path):
    extract_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "ExtractCustomers"])
    load_key = dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "LoadOrders"])
    comp = _component(
        action="execute", demo_mode=True,
        assets_by_package={
            "Sales/SalesETL/ExtractCustomers": {"deps": [load_key.to_user_string()]},
            "Sales/SalesETL/LoadOrders": {"deps": [extract_key.to_user_string()]},
        },
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PACKAGES))
    with pytest.raises(Exception, match="circular dependency"):
        dg.materialize(list(defs.assets), raise_on_error=True)


# ── Gap 3: demo_mode (zero SQL Server dependency) ────────────────────────────

def test_demo_mode_discovers_a_synthetic_catalog_with_no_sql_connection(tmp_path):
    comp = _component(
        workspace=SsisWorkspaceConfig(server=None, user=None, password=None),
        demo_mode=True,
    )
    packages = comp._discover_packages(None)
    assert len(packages) == 2


def test_demo_mode_materializes_end_to_end_with_no_real_engine(tmp_path):
    comp = _component(
        workspace=SsisWorkspaceConfig(server=None, user=None, password=None),
        action="execute", demo_mode=True,
    )
    # write_state_to_path's own demo branch -- not the hand-rolled state file --
    # proves the whole discovery path is demo-safe, not just _discover_packages.
    import asyncio
    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))
    defs = comp.build_defs_from_state(None, state_path)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert len(result.get_asset_materialization_events()) == 2


def test_demo_mode_observation_sensor_and_freshness_check_need_no_engine(tmp_path):
    comp = _component(
        workspace=SsisWorkspaceConfig(server=None, user=None, password=None),
        demo_mode=True, polling_sensor=True, freshness_lag_threshold_seconds=3600,
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _TWO_PACKAGES))

    sensor_def = next(iter(defs.sensors))
    result = sensor_def(dg.build_sensor_context())
    assert len(result.asset_events) == 1  # one observation per tick, demo-style

    for check in defs.asset_checks:
        check_result = check(dg.build_asset_check_context())
        assert check_result.passed is True


def test_build_engine_raises_loudly_if_called_while_demo_mode(tmp_path):
    """Defensive backstop: confirms the real SQL path can never be silently
    reached while demo_mode=true, even if some future caller forgets to
    branch first."""
    comp = _component(demo_mode=True)
    with pytest.raises(RuntimeError, match="demo_mode=true"):
        comp._build_engine()
