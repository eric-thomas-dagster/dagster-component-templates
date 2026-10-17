"""Loads the REAL component.py and the REAL resources/context_receipt_store/component.py by path."""
import importlib.util
import pathlib
import sys
from types import ModuleType


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod  # dataclasses in the store module need to resolve their own module
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(pathlib.Path(__file__).resolve().parent.parent / "component.py", "receipt_outcome_ingestion_under_test")


def load_store_module() -> ModuleType:
    return _load(pathlib.Path(__file__).resolve().parents[4] / "resources" / "context_receipt_store" / "component.py", "context_receipt_store_for_ingestion_tests")
