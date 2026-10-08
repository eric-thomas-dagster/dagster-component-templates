"""Shared test helpers for DefaultWorkflowTriggerComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real network/`requests` calls are
ever made -- `_call_fire_trigger` (the one external, paid-API boundary) is
monkeypatched wholesale in tests, while everything this component
actually owns (dual source resolution, email-vs-responses row mapping,
validation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "default_workflow_trigger_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeDefaultResource:
    """Stands in for the real DefaultResource -- `_call_fire_trigger` is
    monkeypatched wholesale in tests, so `fire_trigger` is never actually
    invoked through this fake; it only needs to exist as a resource key
    target."""

    def fire_trigger(self, trigger_id, email, responses=None, context=None):
        raise AssertionError("fire_trigger should never be called directly in tests -- _call_fire_trigger is monkeypatched")


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
