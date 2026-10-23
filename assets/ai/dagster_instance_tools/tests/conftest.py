"""Loads the real component.py by path (no installed package), and builds a REAL on-disk DagsterInstance with real runs."""
import importlib.util
import pathlib
import sys
from types import ModuleType

import pytest

HERE = pathlib.Path(__file__).resolve().parent


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(HERE.parent / "component.py", "dagster_instance_tools_under_test")


def load_fixture_defs() -> ModuleType:
    return _load(HERE / "fixture_defs.py", "fixture_defs")


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


@pytest.fixture(scope="session")
def world(tmp_path_factory):
    """A temp-dir instance (sqlite under DAGSTER_HOME) with: ok (succeeds), bad (raises + logs a bearer token),
    part (partitions a ok / b fails / c never run), chk (check fails)."""
    import dagster as dg

    fx = load_fixture_defs()
    home = tmp_path_factory.mktemp("dagster_home")
    inst = dg.DagsterInstance.local_temp(str(home))
    r_ok = dg.materialize([fx.ok], instance=inst)
    r_bad = dg.materialize([fx.ok, fx.bad], instance=inst, raise_on_error=False)
    r_pa = dg.materialize([fx.part], partition_key="a", instance=inst)
    r_pb = dg.materialize([fx.part], partition_key="b", instance=inst, raise_on_error=False)
    r_chk = dg.materialize([fx.ok, fx.chk], instance=inst)
    assert r_ok.success and not r_bad.success and r_pa.success and not r_pb.success and r_chk.success
    import shutil
    defs_dir = tmp_path_factory.mktemp("defs_pkg")  # PYTHONPATH is ':'-separated and this repo's path contains ':'
    shutil.copy(HERE / "fixture_defs.py", defs_dir / "fixture_defs.py")
    return {"defs_dir": str(defs_dir), "instance": inst, "home": home, "fx": fx, "bad_run": r_bad.run_id, "ok_run": r_ok.run_id,
            "part_b_run": r_pb.run_id, "chk_run": r_chk.run_id}
