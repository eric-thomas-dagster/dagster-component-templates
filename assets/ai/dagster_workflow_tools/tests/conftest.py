"""Loads the real component.py by path (no installed package), and builds a REAL on-disk
DagsterInstance. `fixture_defs.py` is copied onto sys.path (same convention as
dagster_instance_tools' own conftest.py) so `definitions_module="fixture_defs:defs"` is
importable both by this test process and by `_reconstruct_job` re-importing it -- in-process
execution (`run_config={"execution": {"config": {"in_process": {}}}}`) is forced in every test
that actually executes a job, since a real step subprocess would need to re-import THIS
component.py under its own synthetic test-loader module name, which only this process has
registered (production usage doesn't have this constraint -- the real installed package is
importable from any subprocess)."""
import importlib.util
import pathlib
import shutil
import sys
from types import ModuleType

import pytest

HERE = pathlib.Path(__file__).resolve().parent

IN_PROCESS_RUN_CONFIG = {"execution": {"config": {"in_process": {}}}}


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(HERE.parent / "component.py", "dagster_workflow_tools_under_test")


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


@pytest.fixture()
def instance(tmp_path_factory):
    import dagster as dg
    home = tmp_path_factory.mktemp("dagster_home")
    return dg.DagsterInstance.local_temp(str(home))


@pytest.fixture()
def defs_module_spec(tmp_path_factory, monkeypatch):
    """Copies fixture_defs.py into a fresh tmp dir on sys.path, returns 'fixture_defs:defs'."""
    defs_dir = tmp_path_factory.mktemp("defs_pkg")
    shutil.copy(HERE / "fixture_defs.py", defs_dir / "fixture_defs.py")
    monkeypatch.syspath_prepend(str(defs_dir))
    sys.modules.pop("fixture_defs", None)  # a prior test's copy must not be reused
    return "fixture_defs:defs"
