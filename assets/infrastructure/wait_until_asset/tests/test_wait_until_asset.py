"""Real, committed tests for WaitUntilAssetComponent -- real dg.materialize() throughout,
small poll_seconds/timeout_seconds so the suite stays fast. No mocking: the one external
surface (shell/file/time) is free/local (this repo's "mock only the paid/external call" rule).
"""
import importlib
import sys
import time
from pathlib import Path

import dagster as dg
import pytest

from .conftest import materialize

# `callable_path` is a real dotted-import path (matches production: component.py's
# _resolve_callable() does the same `importlib.import_module(mod_path)`), so it needs
# the repo root on sys.path -- true when pytest runs from the repo root, not guaranteed
# when invoked from elsewhere (e.g. a single component's own directory). Make it robust
# to either by walking up from this file to the repo root (marked by manifest.json) and
# adding it to sys.path ourselves, rather than relying on the caller's cwd.
_repo_root = Path(__file__).resolve()
for _parent in _repo_root.parents:
    if (_parent / "manifest.json").exists():
        _repo_root = _parent
        break
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

FIXTURES_PATH = "assets.infrastructure.wait_until_asset.tests.fixture_callables"


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def test_requires_the_field_for_its_condition(mod):
    with pytest.raises(Exception, match="requires `callable_path`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="callable")
    with pytest.raises(Exception, match="requires `command`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="shell_exit_zero")
    with pytest.raises(Exception, match="requires `file_pattern`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="file_exists")
    with pytest.raises(Exception, match="requires `time_of_day`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="time_of_day")
    with pytest.raises(Exception, match="requires `delay_seconds`"):
        mod.WaitUntilAssetComponent(asset_name="t", condition="delay")


# --- condition='callable' --------------------------------------------------------------

def test_callable_condition_met_immediately(mod, fx):
    fx.reset(true_after=1)
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="callable", callable_path=f"{FIXTURES_PATH}:becomes_true_after_n_calls",
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["met"].value is True
    assert md["attempts"].value == 1


def test_callable_condition_met_after_a_few_polls(mod, fx):
    fx.reset(true_after=3)
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="callable", callable_path=f"{FIXTURES_PATH}:becomes_true_after_n_calls",
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["attempts"].value == 3


def test_callable_condition_raising_is_a_hard_failure(mod, fx):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="callable", callable_path=f"{FIXTURES_PATH}:always_raises",
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component, raise_on_error=False)
    assert not result.success


# --- timeout behavior --------------------------------------------------------------

def test_timeout_fails_by_default(mod, fx):
    fx.reset(true_after=10_000)
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="callable", callable_path=f"{FIXTURES_PATH}:becomes_true_after_n_calls",
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_timeout_skip_materializes_successfully_with_timed_out_metadata(mod, fx):
    fx.reset(true_after=10_000)
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="callable", callable_path=f"{FIXTURES_PATH}:becomes_true_after_n_calls",
        poll_seconds=0.01, timeout_seconds=0.05, on_timeout="skip",
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["timed_out"].value is True
    assert md["met"].value is False


# --- condition='file_exists' ---------------------------------------------------------

def test_file_exists_condition_met_immediately_when_file_already_there(mod, tmp_path):
    f = tmp_path / "ready.csv"
    f.write_text("x")
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="file_exists", file_pattern=str(tmp_path / "*.csv"),
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success


def test_file_exists_condition_times_out_when_no_match(mod, tmp_path):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="file_exists", file_pattern=str(tmp_path / "*.csv"),
        poll_seconds=0.01, timeout_seconds=0.05,
    )
    result = materialize(component, raise_on_error=False)
    assert not result.success


# --- condition='shell_exit_zero' ------------------------------------------------------

def test_shell_exit_zero_met_immediately(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="shell_exit_zero", command="true",
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success


def test_shell_retry_exit_code_keeps_polling_not_failing(mod, tmp_path):
    marker = tmp_path / "ready"
    # Exits 42 (the designated "not ready" code) until `marker` exists, then exits 0.
    cmd = f"[ -f {marker} ] && exit 0 || exit 42"
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="shell_exit_zero", command=cmd, retry_exit_code=42,
        poll_seconds=0.05, timeout_seconds=5,
    )

    import threading
    def _create_marker_soon():
        time.sleep(0.15)
        marker.write_text("x")
    threading.Thread(target=_create_marker_soon, daemon=True).start()

    result = materialize(component)
    assert result.success


def test_shell_non_retry_exit_code_fails_immediately_not_polling(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="shell_exit_zero", command="exit 7", retry_exit_code=42,
        poll_seconds=5, timeout_seconds=30,
    )
    start = time.monotonic()
    result = materialize(component, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 5  # failed on the FIRST check, didn't wait out poll_seconds


# --- condition='delay' ----------------------------------------------------------------

def test_delay_condition_met_once_enough_time_has_passed(mod):
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="delay", delay_seconds=0.1,
        poll_seconds=0.02, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["waited_seconds"].value >= 0.1


# --- condition='time_of_day' -----------------------------------------------------------

def test_time_of_day_already_passed_today_is_met_immediately(mod):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    past = (now.replace(second=0, microsecond=0))
    hhmm = f"{past.hour:02d}:{past.minute:02d}"
    component = mod.WaitUntilAssetComponent(
        asset_name="t", condition="time_of_day", time_of_day=hhmm,
        poll_seconds=0.01, timeout_seconds=5,
    )
    result = materialize(component)
    assert result.success
