"""Real, committed tests for a gap found reviewing a real demo
(chpw/ssis_catalog_workspace.py) that subclassed SsisWorkspaceComponent just
to add a `completed_within_window` asset check: an execution-DURATION SLA
check, distinct from the existing freshness_lag_threshold_seconds check
(which measures staleness -- how long ago a package last succeeded -- not
how long that run took). A package can be perfectly fresh and still have
blown its runtime budget; neither check catches the other's failure mode.
"""
import importlib.util
import json
import pathlib
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("ssis_duration_check", _COMPONENT_PY)
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


class _FakeMappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _FakeMappingsResult(self._rows)


def _fake_engine(rows):
    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def execute(self, *a, **k):
            return _FakeExecuteResult(rows)

    class _Engine:
        def connect(self):
            return _Conn()

    return _Engine()


def test_demo_mode_duration_check_needs_no_engine(tmp_path):
    comp = _component(
        workspace=SsisWorkspaceConfig(server=None, user=None, password=None),
        demo_mode=True, max_execution_minutes=30,
    )
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    check = next(iter(defs.asset_checks))
    result = check(dg.build_asset_check_context())
    assert result.passed is True


def test_duration_check_passes_when_within_window(tmp_path, monkeypatch):
    comp = _component(max_execution_minutes=30)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    check = next(iter(defs.asset_checks))

    start = datetime(2024, 3, 15, 2, 0, tzinfo=timezone.utc)
    rows = [{"start_time": start, "end_time": start + timedelta(minutes=12)}]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine(rows))

    result = check(dg.build_asset_check_context())
    assert result.passed is True
    assert result.metadata["ssis/minutes_taken"].value == 12.0


def test_duration_check_fails_when_over_window(tmp_path, monkeypatch):
    comp = _component(max_execution_minutes=30)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    check = next(iter(defs.asset_checks))

    start = datetime(2024, 3, 15, 2, 0, tzinfo=timezone.utc)
    rows = [{"start_time": start, "end_time": start + timedelta(minutes=45)}]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine(rows))

    result = check(dg.build_asset_check_context())
    assert result.passed is False
    assert result.metadata["ssis/minutes_taken"].value == 45.0


def test_duration_check_and_freshness_check_are_independent(tmp_path, monkeypatch):
    """A package that is fresh (just succeeded) but took too long to run
    fails the duration check while the freshness check still passes --
    confirms these are two distinct checks, not one renamed."""
    comp = _component(max_execution_minutes=30, freshness_lag_threshold_seconds=3600)
    defs = comp.build_defs_from_state(None, _write_state(tmp_path, _ONE_PACKAGE))
    checks_by_name = {next(iter(c.check_keys)).name: c for c in defs.asset_checks}
    assert set(checks_by_name) == {"ssis_freshness_lag", "ssis_completed_within_window"}

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=50)
    rows = [{"start_time": start, "end_time": now}]
    monkeypatch.setattr(comp, "_build_engine", lambda: _fake_engine(rows))

    duration_result = checks_by_name["ssis_completed_within_window"](dg.build_asset_check_context())
    freshness_result = checks_by_name["ssis_freshness_lag"](dg.build_asset_check_context())
    assert duration_result.passed is False  # took 50 min, budget is 30
    assert freshness_result.passed is True  # just finished -- not stale
