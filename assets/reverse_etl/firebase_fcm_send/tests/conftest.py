"""Shared test helpers for FirebaseFcmSendComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `firebase-admin` package is
never installed or imported -- `_call_firebase_api` (the one external
boundary) is monkeypatched wholesale in tests, while everything this
component actually owns (dual source resolution, template rendering,
validation, per-row failure isolation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "firebase_fcm_send_component", component_py
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


class FakeFirebaseResource:
    """Stands in for the real FirebaseResource -- `.get_app()` is never
    actually invoked since `_call_firebase_api` is monkeypatched wholesale
    in tests."""

    def get_app(self):
        return None


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
