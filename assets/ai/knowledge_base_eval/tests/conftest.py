"""Loads the real component.py by path (no installed package)."""
import importlib.util
import pathlib
from types import ModuleType

import pytest

HERE = pathlib.Path(__file__).resolve().parent


def load_component_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("knowledge_base_eval_under_test", HERE.parent / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()
