"""Real, committed tests for four gaps found subclassing SsisWorkspaceComponent
(component-feedback/2026-09-24-ssis-workspace-gaps.md):

1. No `partitions_def` field at all -- added, threaded into the translator's
   AssetSpec and (best-effort, time-based types only) into the observation
   sensor's AssetObservation(partition=...).
2. The observation sensor ignored get_asset_spec's key override, rebuilding
   [*prefix, folder, project, pkg] independently instead of calling
   self.get_asset_spec(props).key (the component's own documented override
   point). Same class of bug found (and fixed, this session) in
   snowflake_workspace, azure_data_factory, and fabric_workspace.
3. freshness_lag_threshold_seconds's check couldn't be evaluated through a
   job when action: noop (no executable asset to attach to) -- noop now
   builds a genuine no-op @dg.multi_asset instead of bare AssetSpecs.
4. _build_freshness_checks's per-package closure used a default-argument
   trick Dagster's asset-check machinery misreads as a real input,
   breaking the moment the project's default io_manager is type-specific --
   replaced with a real factory-function closure.
"""
import importlib.util
import json
import pathlib
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("ssis_four_gaps", _COMPONENT_PY)
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


_ONE_PACKAGE = [{
    "folder_name": "Finance", "project_name": "Nightly", "project_id": 1,
    "package_name": "LoadGL", "description": None,
}]


# ── Gap 1: partitions_def ----------------------------------------------------

def test_no_partition_type_is_unpartitioned_by_default(tmp_path):
    comp = _component()
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    spec = next(iter(defs.assets)).get_asset_spec(dg.AssetKey(["ssis", "sql-01", "Finance", "Nightly", "LoadGL"]))
    assert spec.partitions_def is None


def test_partition_type_daily_attaches_a_real_partitions_def(tmp_path):
    comp = _component(partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    spec = next(iter(defs.assets)).get_asset_spec(dg.AssetKey(["ssis", "sql-01", "Finance", "Nightly", "LoadGL"]))
    assert isinstance(spec.partitions_def, dg.DailyPartitionsDefinition)


def test_partition_type_daily_without_start_raises():
    comp = _component(partition_type="daily")
    with pytest.raises(ValueError, match="partition_start"):
        comp._partitions_def


# ── Gap 2: sensor key-override ------------------------------------------------

class _FakeConn:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, *a, **k):
        return self


class _FakeMappingsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


def _fake_engine_with_executions(rows):
    class _Engine:
        def connect(self):
            conn = _FakeConn(rows)

            def _execute(*a, **k):
                return _FakeMappingsResultWrapper(rows)
            conn.execute = _execute
            return conn
    return _Engine()


class _FakeMappingsResultWrapper:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _FakeMappingsResult(self._rows)


def test_sensor_observes_against_the_overridden_key_not_a_rederived_default(tmp_path, monkeypatch):
    def _translate(base_spec, props):
        return base_spec._replace(key=dg.AssetKey(["custom", props.package_name]))

    comp = _component(polling_sensor=True, translation=_translate)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    sensor_def = next(iter(defs.sensors))

    rows = [{
        "execution_id": 1, "folder_name": "Finance", "project_name": "Nightly",
        "package_name": "LoadGL.dtsx", "status": 7, "end_time": datetime(2024, 3, 15, tzinfo=timezone.utc),
    }]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine_with_executions(rows))

    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert len(result.asset_events) == 1
    assert result.asset_events[0].asset_key == dg.AssetKey(["custom", "LoadGL"])


def test_sensor_tags_a_time_partition_from_execution_end_time(tmp_path, monkeypatch):
    comp = _component(polling_sensor=True, partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    sensor_def = next(iter(defs.sensors))

    rows = [{
        "execution_id": 1, "folder_name": "Finance", "project_name": "Nightly",
        "package_name": "LoadGL.dtsx", "status": 7, "end_time": datetime(2024, 3, 15, 12, 0, tzinfo=timezone.utc),
    }]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine_with_executions(rows))

    result = sensor_def(dg.build_sensor_context())
    assert result.asset_events[0].partition == "2024-03-15"


# ── Gap 3: noop check-executability ------------------------------------------

def test_noop_action_builds_a_real_executable_not_a_bare_asset_spec(tmp_path):
    comp = _component(action="noop", freshness_lag_threshold_seconds=3600)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success


def test_noop_check_runs_through_a_real_job_selection(tmp_path, monkeypatch):
    """This is the exact crash from the report: building a job for a
    check-only selection over a noop package used to raise
    DagsterInvalidDefinitionError. Confirm it now runs cleanly."""
    comp = _component(action="noop", freshness_lag_threshold_seconds=3600)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))

    rows = [{"end_time": datetime.now(timezone.utc) - timedelta(seconds=10)}]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine_first(rows[0]))

    key = dg.AssetKey(["ssis", "sql-01", "Finance", "Nightly", "LoadGL"])
    defs_obj = dg.Definitions(assets=list(defs.assets), asset_checks=list(defs.asset_checks or []))
    result = defs_obj.resolve_implicit_job_def_def_for_assets([key]).execute_in_process(
        asset_selection=[key], raise_on_error=True,
    )
    assert result.success


class _FakeConnFirst:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, *a, **k):
        return self

    def mappings(self):
        return self

    def first(self):
        return self.row


def _fake_engine_first(row):
    class _Engine:
        def connect(self):
            return _FakeConnFirst(row)
    return _Engine()


# ── Gap 4: closure late-binding under multiple packages -----------------------

def test_freshness_check_closure_is_correct_per_package_not_the_last_loop_value(tmp_path, monkeypatch):
    """Confirms _make_check's factory closes over the RIGHT spec per package --
    the old default-argument trick's failure mode (if it had been merely
    reverted to a naive closure instead) would make every check report the
    LAST package's metadata."""
    packages = [
        {"folder_name": "Finance", "project_name": "Nightly", "project_id": 1, "package_name": "LoadGL", "description": None},
        {"folder_name": "Sales", "project_name": "Nightly", "project_id": 2, "package_name": "LoadOrders", "description": None},
    ]
    comp = _component(action="noop", freshness_lag_threshold_seconds=3600)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, packages))

    calls = []

    class _Engine:
        def connect(self_inner):
            class _Conn:
                def __enter__(self): return self
                def __exit__(self, *a): pass
                def execute(self, stmt, params):
                    calls.append(params["package"])
                    return self
                def mappings(self): return self
                def first(self): return {"end_time": datetime.now(timezone.utc)}
            return _Conn()

    monkeypatch.setattr(comp, "_build_engine", lambda: _Engine())

    for check in defs.asset_checks:
        ctx = dg.build_asset_check_context()
        check(ctx)

    assert sorted(calls) == ["LoadGL.dtsx", "LoadOrders.dtsx"]
