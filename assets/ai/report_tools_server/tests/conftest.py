"""Loads the real component.py by path (no installed package)."""
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
    return _load(HERE.parent / "component.py", "report_tools_server_under_test")


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


@pytest.fixture()
def files_dir(tmp_path):
    return str(tmp_path / "report_files")
