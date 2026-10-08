"""Shared test helpers for VantaEvidenceUploadComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real network/`requests` calls are
ever made -- `_create_document` / `_upload_file` / `_submit_document` (the
three external, paid-API boundaries) are monkeypatched wholesale in tests,
while everything this component actually owns (dual source resolution,
file resolution, validation, per-row aggregation, metadata) is exercised
for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "vanta_evidence_upload_component", component_py
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


class FakeVantaResource:
    """Stands in for the real VantaResource -- `_create_document` /
    `_upload_file` / `_submit_document` are monkeypatched wholesale in
    tests, so `get_client()` never actually sends anything anywhere."""

    api_base_url = "https://api.vanta.com"

    def get_client(self):
        return object()


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
