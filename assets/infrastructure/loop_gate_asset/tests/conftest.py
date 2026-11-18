"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("loop_gate_asset_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


def step_statuses(result):
    out = {}
    for ev in result.all_events:
        if ev.event_type_value in ("STEP_SKIPPED", "STEP_SUCCESS", "STEP_FAILURE"):
            out[ev.step_key] = ev.event_type_value
    return out
