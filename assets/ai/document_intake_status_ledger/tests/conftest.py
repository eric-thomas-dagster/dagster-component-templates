"""Loads the real component.py by path (this repo's convention: no package install needed to test)."""
import importlib.util
import pathlib
from types import ModuleType


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(pathlib.Path(__file__).resolve().parent.parent / "component.py", "document_intake_status_ledger_under_test")
