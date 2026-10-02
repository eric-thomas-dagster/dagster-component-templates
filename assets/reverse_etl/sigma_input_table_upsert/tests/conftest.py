"""Shared test helpers for SigmaInputTableUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real network/`requests` calls are
ever made -- `FakeSigmaResource` stands in for the one external, paid-API
boundary (`resource.get()` / `resource.post()`), while everything this
component actually owns (dual source resolution, fields_map row-building,
schema pre-flight, chunking, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "sigma_input_table_upsert_component", component_py
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


class FakeSigmaResource:
    """Stands in for the real SigmaResource. Records every `.get()` /
    `.post()` call and serves canned responses."""

    def __init__(self, schema_response=None, post_response=None):
        self._schema_response = schema_response if schema_response is not None else {
            "variables": {"rows": {"type": "array"}}
        }
        self._post_response = post_response if post_response is not None else {"traceId": "trace-1"}
        self.get_calls = []
        self.post_calls = []

    def get(self, path, params=None):
        self.get_calls.append({"path": path, "params": params})
        return self._schema_response

    def post(self, path, json_body=None):
        self.post_calls.append({"path": path, "json_body": json_body})
        return self._post_response


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
