"""Shared test helpers for EventAutomationComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. All fixtures use ephemeral Dagster instances +
mocked HTTP / SMTP clients — no external services touched.
"""
import importlib.util
import pathlib
import sys
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "event_automation_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["event_automation_component"] = mod
    spec.loader.exec_module(mod)
    return mod
