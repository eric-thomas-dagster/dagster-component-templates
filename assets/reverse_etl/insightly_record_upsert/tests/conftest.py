"""Shared test helpers for InsightlyRecordUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. A minimal FakeInsightlyResource stands
in for the real InsightlyResource -- the one external, paid-API boundary
-- so everything this component actually owns (dual source resolution,
validation, CONTACTINFOS shaping, dedupe-column resolution, metadata
counting) is exercised for real, matching this repo's "mock only the
external call" convention (see google_ads_customer_match_upsert/tests).
"""
import importlib.util
import pathlib
from types import ModuleType
from typing import Optional

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "insightly_record_upsert_component", component_py
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


class FakeInsightlyResource:
    """Stands in for the real InsightlyResource -- records every call so
    tests can assert on search_field_name / search_value / body shape
    without touching the network. `existing_by_value` seeds which
    dedupe values should resolve to an existing record (update path)."""

    def __init__(self, existing_by_value: Optional[dict] = None):
        self.existing_by_value = existing_by_value or {}
        self.upsert_calls = []
        self._next_id = 1000

    def upsert(self, object_type, search_field_name, search_field_value, body):
        self.upsert_calls.append(
            {
                "object_type": object_type,
                "search_field_name": search_field_name,
                "search_field_value": search_field_value,
                "body": body,
            }
        )
        if search_field_value in self.existing_by_value:
            return {"action": "updated", "id": self.existing_by_value[search_field_value]}
        self._next_id += 1
        return {"action": "created", "id": self._next_id}


class FailingInsightlyResource(FakeInsightlyResource):
    """Raises for a configured set of dedupe values -- used to exercise
    the per-row error-isolation path."""

    def __init__(self, fail_values: set):
        super().__init__()
        self.fail_values = fail_values

    def upsert(self, object_type, search_field_name, search_field_value, body):
        if search_field_value in self.fail_values:
            raise RuntimeError(f"simulated Insightly failure for {search_field_value}")
        return super().upsert(object_type, search_field_name, search_field_value, body)
