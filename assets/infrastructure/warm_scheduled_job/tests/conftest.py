"""Shared test helpers for WarmScheduledJobComponent."""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "warm_scheduled_job_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _croniter_available() -> bool:
    try:
        import croniter  # noqa: F401
        return True
    except ImportError:
        return False


requires_croniter = pytest.mark.skipif(
    not _croniter_available(),
    reason="requires croniter (pip install croniter)",
)
