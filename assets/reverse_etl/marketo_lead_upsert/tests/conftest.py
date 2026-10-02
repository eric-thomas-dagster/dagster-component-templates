"""Shared test helpers for MarketoLeadUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `_call_marketo_leads_api` (the one
external, paid-API boundary -- Marketo's `POST /rest/v1/leads.json`) is
monkeypatched wholesale in tests, while everything this component actually
owns (dual source resolution, fields_map application, lookup validation,
chunking, create/update/skipped accounting, metadata) is exercised for
real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "marketo_lead_upsert_component", component_py
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


class FakeMarketoResource:
    """Stands in for the real MarketoResource -- `.post()` is never
    actually invoked since `_call_marketo_leads_api` is monkeypatched
    wholesale in tests."""

    def post(self, path, json_body=None):
        raise AssertionError("FakeMarketoResource.post() should never be called directly in tests")


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
