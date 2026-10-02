"""Shared test helpers for MetronomeUsageEventSendComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `_call_metronome_ingest` (the one
external, paid-API boundary) is monkeypatched wholesale in tests, while
everything this component actually owns (dual source resolution, event
construction, transaction_id derivation, batching, retry/backoff
semantics) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "metronome_usage_event_send_component", component_py
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


class FakeMetronomeResource:
    """Stands in for the real MetronomeResource -- `.get_client()` is never
    actually invoked since `_call_metronome_ingest` is monkeypatched
    wholesale in tests."""

    base_url = "https://api.metronome.com/v1"

    def get_client(self):
        return None


class FakeResponse:
    def __init__(self, status_code: int, body=None, headers=None, text: str = ""):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = text or ""

    def json(self):
        return self._body


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
