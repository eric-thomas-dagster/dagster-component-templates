"""Shared test helpers for CloseCrmLeadUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. A minimal fake CloseCrmResource stands
in for the real HTTP-backed one (the external, paid-API boundary lives in
close_crm_resource, not here) -- matching the repo's convention of mocking
only the external call while exercising all of this component's own logic
(dual source resolution, validation, Lead-body building, dedupe-value
extraction, metadata counting) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "close_crm_lead_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeCloseCrmResource:
    """Stands in for the real CloseCrmResource -- records every
    upsert_lead() call and plays back scripted results (created / updated /
    raise) keyed by dedupe value, without ever touching the network."""

    def __init__(self, scripted_results: dict | None = None):
        self.scripted_results = scripted_results or {}
        self.upsert_calls: list[dict] = []

    def upsert_lead(self, dedupe_field, dedupe_value, create_body, update_body=None):
        self.upsert_calls.append(
            {
                "dedupe_field": dedupe_field,
                "dedupe_value": dedupe_value,
                "create_body": create_body,
                "update_body": update_body,
            }
        )
        scripted = self.scripted_results.get(dedupe_value)
        if scripted is not None:
            if isinstance(scripted, Exception):
                raise scripted
            return scripted
        # Default: always "created" unless the test scripted otherwise.
        return {"action": "created", "id": f"lead_{dedupe_value}"}
