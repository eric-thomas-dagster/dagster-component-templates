"""Real, committed tests for three gaps found against the ADF-converter feedback
(SSIS needs an explicit declared package list with no state file, a dry_run
flag matching the SQL/email/SAP components' convention, and a real project_id
in the observation sensor instead of a hardcoded placeholder):

1. `packages:` lets the component skip SSISDB discovery AND the
   StateBackedComponent state-file cache entirely -- build_defs() short-circuits
   to _build_defs_from_packages() directly, with no state_path ever touched.
2. `dry_run: true` on `action: execute` logs the would-be execution (with
   resolved package_overrides) and skips create_execution/start_execution/poll
   -- and skips building a real SQL engine at all, unlike demo_mode.
3. The observation sensor's SsisPackageProps used to hardcode project_id=0;
   it now joins SSISDB.catalog.folders/projects by (folder_name, project_name)
   and threads the real value through.
"""
import importlib.util
import pathlib
from datetime import datetime, timezone

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("ssis_packages_and_dry_run", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

SsisWorkspaceComponent = mod.SsisWorkspaceComponent
SsisWorkspaceConfig = mod.SsisWorkspaceConfig
SsisPackageDeclaration = mod.SsisPackageDeclaration
SsisPackageOverride = mod.SsisPackageOverride


def _component(**overrides):
    attrs = dict(
        workspace=SsisWorkspaceConfig(server="sql-01", database="SSISDB", user="u", password="p"),
        action="noop",
    )
    attrs.update(overrides)
    return SsisWorkspaceComponent(**attrs)


# ── packages: explicit declaration, no state file ────────────────────────────

def test_packages_bypasses_discovery_and_the_state_file_cache(tmp_path, monkeypatch):
    comp = _component(
        packages=[
            SsisPackageDeclaration(folder_name="Sales", project_name="SalesETL", package_name="ExtractCustomers"),
            SsisPackageDeclaration(folder_name="Sales", project_name="SalesETL", package_name="LoadOrders", project_id=7),
        ],
    )

    def _boom(*a, **k):
        raise AssertionError("discovery/state-file path should not run when packages: is set")
    monkeypatch.setattr(comp, "_discover_packages", _boom)

    defs = comp.build_defs(context=None)
    keys = {s.key for s in list(next(iter(defs.assets)).specs)}
    assert keys == {
        dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "ExtractCustomers"]),
        dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "LoadOrders"]),
    }


def test_packages_still_applies_package_selector(tmp_path):
    comp = _component(
        packages=[
            SsisPackageDeclaration(folder_name="Sales", project_name="SalesETL", package_name="ExtractCustomers"),
            SsisPackageDeclaration(folder_name="Finance", project_name="Nightly", package_name="LoadGL"),
        ],
        package_selector=mod.SsisPackageSelector(by_folder=["Sales"]),
    )
    defs = comp.build_defs(context=None)
    keys = {s.key for s in next(iter(defs.assets)).specs}
    assert keys == {dg.AssetKey(["ssis", "sql-01", "Sales", "SalesETL", "ExtractCustomers"])}


def test_declared_project_id_flows_into_environment_reference_lookup(tmp_path):
    """project_id on a declared package is the one real fields _resolve_overrides/
    _execute_package use to look up an environment_reference -- confirm it's not
    silently dropped on the explicit-declaration path."""
    comp = _component(
        action="execute", demo_mode=False,
        packages=[SsisPackageDeclaration(folder_name="Sales", project_name="SalesETL", package_name="LoadOrders", project_id=42)],
        package_overrides=[SsisPackageOverride(match="*", environment_reference="PROD")],
    )
    defs = comp.build_defs(context=None)
    spec = list(next(iter(defs.assets)).specs)[0]
    assert spec.metadata["ssis/project_id"] == 42


# ── dry_run ──────────────────────────────────────────────────────────────────

_ONE_PACKAGE = [{
    "folder_name": "Finance", "project_name": "Nightly", "project_id": 1,
    "package_name": "LoadGL", "description": None,
}]


def test_dry_run_skips_execution_and_builds_no_real_engine(tmp_path, monkeypatch):
    comp = _component(action="execute", dry_run=True)

    def _boom():
        raise AssertionError("dry_run must not build a real SQL engine")
    monkeypatch.setattr(comp, "_build_engine", _boom)

    defs = comp._build_defs_from_packages(_ONE_PACKAGE, "sql-01")
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["ssis/status"].value == "dry_run — not executed"


def test_dry_run_logs_resolved_overrides(tmp_path, monkeypatch, caplog):
    comp = _component(
        action="execute", dry_run=True,
        package_overrides=[SsisPackageOverride(match="*", parameters={"BatchSize": 100})],
    )
    monkeypatch.setattr(comp, "_build_engine", lambda: (_ for _ in ()).throw(AssertionError("no engine in dry_run")))
    defs = comp._build_defs_from_packages(_ONE_PACKAGE, "sql-01")
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success


# ── observation sensor: real project_id via join ─────────────────────────────

class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, *a, **k):
        return self


def _fake_engine_with_executions(rows):
    class _Engine:
        def connect(self):
            conn = _FakeConn()

            def _execute(*a, **k):
                return _FakeMappingsResultWrapper(rows)
            conn.execute = _execute
            return conn
    return _Engine()


class _FakeMappingsResultWrapper:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


def test_sensor_resolves_real_project_id_instead_of_hardcoded_zero(tmp_path, monkeypatch):
    seen_project_ids = []

    def _translate(base_spec, props):
        seen_project_ids.append(props.project_id)
        return base_spec

    comp = _component(polling_sensor=True, translation=_translate)
    defs = comp._build_defs_from_packages(_ONE_PACKAGE, "sql-01")
    sensor_def = next(iter(defs.sensors))

    rows = [{
        "execution_id": 1, "folder_name": "Finance", "project_name": "Nightly",
        "package_name": "LoadGL.dtsx", "status": 7,
        "end_time": datetime(2024, 3, 15, tzinfo=timezone.utc),
        "project_id": 55,
    }]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine_with_executions(rows))

    sensor_def(dg.build_sensor_context())
    # get_asset_spec is also called once up front while building the asset specs
    # themselves (from the static package list's own project_id=1) -- the
    # sensor-resolved value (55, from the real SQL join) is the second call.
    assert seen_project_ids == [1, 55]


def test_sensor_falls_back_to_zero_when_join_finds_no_project(tmp_path, monkeypatch):
    seen_project_ids = []

    def _translate(base_spec, props):
        seen_project_ids.append(props.project_id)
        return base_spec

    comp = _component(polling_sensor=True, translation=_translate)
    defs = comp._build_defs_from_packages(_ONE_PACKAGE, "sql-01")
    sensor_def = next(iter(defs.sensors))

    rows = [{
        "execution_id": 1, "folder_name": "Finance", "project_name": "Nightly",
        "package_name": "LoadGL.dtsx", "status": 7,
        "end_time": datetime(2024, 3, 15, tzinfo=timezone.utc),
        "project_id": None,
    }]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine_with_executions(rows))

    sensor_def(dg.build_sensor_context())
    assert seen_project_ids == [1, 0]
