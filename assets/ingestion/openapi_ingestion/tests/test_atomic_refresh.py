"""Refreshing OpenAPIIngestionComponent state must never expose missing/partial state to a concurrent reader.

Background (see enriched_dbt_project commit 21864db9): the stock StateBackedComponent.refresh_state
rmtree()s the state path, THEN runs write_state_to_path (slow: network calls), so a reader polling
during the refresh sees nothing (empty Definitions) or a half-written file (JSONDecodeError).
The component's refresh_state builds the new state beside the old and renames it into place.

Only the slow *fetch* is stubbed (write_state_to_path of a test subclass); the refresh machinery
under test -- the component's real refresh_state vs the framework's stock one -- is real, on real
files in tmp_path.
"""
import asyncio
import json
import threading
import time
from pathlib import Path

from dagster._core.storage.defs_state.base import DefsStateStorage
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_shared.serdes.objects.models.defs_state_info import DefsStateManagementType

from .conftest import load_component_module

KEY = "AtomicRefreshTest"
IS_DIR_STATE = False


def _make_component():
    mod = load_component_module()
    base = mod.OpenAPIIngestionComponent
    assert "refresh_state" in vars(base), "component must define its own atomic refresh_state"

    class _Slow(base):  # type: ignore[misc, valid-type]
        @property
        def defs_state_config(self):
            return DefsStateConfig(
                key=KEY,
                management_type=DefsStateManagementType.LOCAL_FILESYSTEM,
                refresh_if_dev=False,
            )

        def write_state_to_path(self, state_path: Path) -> None:
            # Stand-in for the slow network fetch + non-atomic write real components do.
            time.sleep(0.15)
            if IS_DIR_STATE:
                state_path.mkdir(parents=True)
                (state_path / "a.json").write_text('{"v": 2')  # partial
                time.sleep(0.15)
                (state_path / "a.json").write_text(json.dumps({"v": 2}))
            else:
                state_path.write_text('{"v": 2')  # partial JSON, as a crash/concurrent read would see
                time.sleep(0.15)
                state_path.write_text(json.dumps({"v": 2}))

    return _Slow, object.__new__(_Slow)


def _setup(tmp_path, monkeypatch) -> Path:
    """Point the framework's local-state path at tmp_path (no dg project needed) and use no state storage."""
    import dagster.components.utils.project_paths as pp

    state_path = tmp_path / ".local_defs_state" / KEY / "state"
    monkeypatch.setattr(pp, "get_local_state_path", lambda key, root: state_path)
    # the stock refresh imported the function by name, so patch that binding too
    import dagster.components.component.state_backed_component as sbc

    monkeypatch.setattr(sbc, "get_local_state_path", lambda key, root: state_path)
    monkeypatch.setattr(DefsStateStorage, "get", classmethod(lambda cls: None))
    return state_path


def _read_state(state_path: Path):
    """What a reader (build_defs_from_state) would see: None if missing, raises if partial."""
    target = state_path / "a.json" if IS_DIR_STATE else state_path
    if not target.exists():
        return None
    return json.loads(target.read_text())["v"]


def _seed(state_path: Path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    if IS_DIR_STATE:
        state_path.mkdir()
        (state_path / "a.json").write_text(json.dumps({"v": 1}))
    else:
        state_path.write_text(json.dumps({"v": 1}))


def _poll_during(refresh_coro_fn, state_path: Path):
    """Run refresh in a thread while polling the state like a concurrent reader; return observations."""
    seen: list = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                seen.append(_read_state(state_path))
            except Exception as exc:  # partial JSON etc.
                seen.append(f"ERR:{type(exc).__name__}")
            time.sleep(0.002)

    t = threading.Thread(target=reader)
    t.start()
    try:
        asyncio.run(refresh_coro_fn())
    finally:
        stop.set()
        t.join()
    return seen


def test_atomic_refresh_never_exposes_missing_or_partial_state(tmp_path, monkeypatch):
    state_path = _setup(tmp_path, monkeypatch)
    _, comp = _make_component()
    _seed(state_path)
    seen = _poll_during(lambda: comp.refresh_state(tmp_path), state_path)
    assert seen, "reader never ran"
    assert set(seen) <= {1, 2}, f"reader observed missing/partial state: {sorted(set(map(str, seen)))}"
    assert _read_state(state_path) == 2  # the new state did land
    leftovers = [p.name for p in state_path.parent.iterdir() if p.name != state_path.name]
    assert leftovers == [], f"temp artifacts left behind: {leftovers}"


def test_stock_refresh_would_expose_missing_or_partial_state(tmp_path, monkeypatch):
    """Control: the stock framework refresh (what this component had before) is observably non-atomic."""
    state_path = _setup(tmp_path, monkeypatch)
    _, comp = _make_component()
    _seed(state_path)
    seen = _poll_during(lambda: StateBackedComponent.refresh_state(comp, tmp_path), state_path)
    assert (None in seen) or any(isinstance(s, str) for s in seen), (
        "expected the stock refresh to expose a missing/partial state window"
    )


def test_failed_refresh_keeps_previous_state(tmp_path, monkeypatch):
    state_path = _setup(tmp_path, monkeypatch)
    cls, _ = _make_component()

    class _Boom(cls):  # type: ignore[misc, valid-type]
        def write_state_to_path(self, state_path: Path) -> None:
            if IS_DIR_STATE:
                state_path.mkdir(parents=True)
            else:
                state_path.write_text('{"v": 2')
            raise RuntimeError("upstream API down")

    comp = object.__new__(_Boom)
    _seed(state_path)
    try:
        asyncio.run(comp.refresh_state(tmp_path))
    except RuntimeError:
        pass
    else:
        raise AssertionError("refresh should have raised")
    assert _read_state(state_path) == 1
    assert [p.name for p in state_path.parent.iterdir()] == [state_path.name]
