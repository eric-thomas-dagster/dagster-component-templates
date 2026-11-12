"""Shared test helpers for event_log_to_snowflake_job (self-contained; loads component.py via importlib).

Only the external service boundary is faked; the component runs for real under dg.materialize()/execute_in_process().
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("event_log_to_snowflake_job_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
