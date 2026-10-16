"""The 18 workspace/integration StateBackedComponents refresh their state atomically.

The stock StateBackedComponent.refresh_state rmtree()s the state path and only then runs write_state_to_path
(seconds of network + a non-atomic write), so a concurrent reader (run worker, `dg list defs`) can see missing
or half-written state. Each component overrides refresh_state to build the new state beside the old and swap it
in. Only the slow *fetch* is stubbed; the refresh machinery under test is real, on real files in tmp_path.
(Same checks as the per-component tests for the 9 asset-level components, e.g. assets/analytics/dataiku_asset.)
"""
import asyncio
import importlib.util
import inspect
import json
import threading
import time
from pathlib import Path

import pytest
from dagster._core.storage.defs_state.base import DefsStateStorage
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_shared.serdes.objects.models.defs_state_info import DefsStateManagementType

ROOT = Path(__file__).resolve().parent.parent
NAMES = (
    "mlflow_workspace coalesce_workspace ssis_workspace prefect_workspace qlik_compose_workspace aws_glue "
    "informatica_workspace azure_data_factory wandb_workspace snowflake_workspace hvr_hub_workspace "
    "informatica_powercenter_workspace fabric_workspace talend_cloud_workspace qlik_replicate_workspace "
    "cognos_workspace jde_orchestrator_workspace tm1_workspace"
).split()
KEY = "AtomicRefreshTest"


def _component_class(name):
    path = ROOT / "integrations" / name / "component.py"
    spec = importlib.util.spec_from_file_location(f"_iar_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except ImportError as e:  # optional SDK not installed
        pytest.skip(f"{name}: {e}")
    cls = [c for c in vars(mod).values() if inspect.isclass(c) and "refresh_state" in vars(c)
           and issubclass(c, StateBackedComponent) and c.__module__ == mod.__name__]
    assert len(cls) == 1, f"{name}: expected exactly one class with its own refresh_state, got {cls}"
    return cls[0]


def _subclass(base, fetch):
    class _T(base):  # type: ignore[misc, valid-type]
        @property
        def defs_state_config(self):
            return DefsStateConfig(key=KEY, management_type=DefsStateManagementType.LOCAL_FILESYSTEM, refresh_if_dev=False)

        def write_state_to_path(self, state_path: Path) -> None:
            fetch(state_path)

    return object.__new__(_T)


def _setup(tmp_path, monkeypatch) -> Path:
    import dagster.components.component.state_backed_component as sbc
    import dagster.components.utils.project_paths as pp

    state_path = tmp_path / ".local_defs_state" / KEY / "state"
    monkeypatch.setattr(pp, "get_local_state_path", lambda key, root: state_path)
    monkeypatch.setattr(sbc, "get_local_state_path", lambda key, root: state_path)
    monkeypatch.setattr(DefsStateStorage, "get", classmethod(lambda cls: None))
    return state_path


def _slow_fetch(state_path: Path):
    time.sleep(0.15)
    state_path.write_text('{"v": 2')  # partial JSON, as a crash/concurrent read would see
    time.sleep(0.15)
    state_path.write_text(json.dumps({"v": 2}))


def _read(state_path: Path):
    return json.loads(state_path.read_text())["v"] if state_path.exists() else None


def _seed(state_path: Path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"v": 1}))


def _poll_during(refresh, state_path: Path):
    seen, stop = [], threading.Event()

    def reader():
        while not stop.is_set():
            try:
                seen.append(_read(state_path))
            except Exception as exc:
                seen.append(f"ERR:{type(exc).__name__}")
            time.sleep(0.002)

    t = threading.Thread(target=reader)
    t.start()
    try:
        asyncio.run(refresh())
    finally:
        stop.set()
        t.join()
    return seen


@pytest.mark.parametrize("name", NAMES)
def test_atomic_refresh_never_exposes_missing_or_partial_state(name, tmp_path, monkeypatch):
    state_path = _setup(tmp_path, monkeypatch)
    comp = _subclass(_component_class(name), _slow_fetch)
    _seed(state_path)
    seen = _poll_during(lambda: comp.refresh_state(tmp_path), state_path)
    assert seen and set(seen) <= {1, 2}, f"reader observed missing/partial state: {sorted(set(map(str, seen)))}"
    assert _read(state_path) == 2
    assert [p.name for p in state_path.parent.iterdir() if p.name != state_path.name] == []


@pytest.mark.parametrize("name", NAMES)
def test_stock_refresh_is_observably_non_atomic(name, tmp_path, monkeypatch):
    """Control: without the override (the framework's stock refresh) the same reader sees a gap."""
    state_path = _setup(tmp_path, monkeypatch)
    comp = _subclass(_component_class(name), _slow_fetch)
    _seed(state_path)
    seen = _poll_during(lambda: StateBackedComponent.refresh_state(comp, tmp_path), state_path)
    assert (None in seen) or any(isinstance(s, str) for s in seen)


@pytest.mark.parametrize("name", NAMES)
def test_failed_refresh_keeps_previous_state(name, tmp_path, monkeypatch):
    state_path = _setup(tmp_path, monkeypatch)

    def boom(path: Path):
        path.write_text('{"v": 2')
        raise RuntimeError("upstream API down")

    comp = _subclass(_component_class(name), boom)
    _seed(state_path)
    with pytest.raises(RuntimeError):
        asyncio.run(comp.refresh_state(tmp_path))
    assert _read(state_path) == 1
    assert [p.name for p in state_path.parent.iterdir()] == [state_path.name]
