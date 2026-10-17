"""Loads the real component.py by path; also loads agentic_pipeline so tests use the REAL `approve` op to write pending records."""
import importlib.util
import pathlib
import sys
from types import ModuleType


def _load(path: pathlib.Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_component_module() -> ModuleType:
    return _load(pathlib.Path(__file__).resolve().parent.parent / "component.py", "agent_review_queue_under_test")


def load_agentic_module() -> ModuleType:
    return _load(pathlib.Path(__file__).resolve().parents[2] / "agentic_pipeline" / "component.py", "agentic_pipeline_for_queue_tests")
