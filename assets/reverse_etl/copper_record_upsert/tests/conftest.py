"""Shared test helpers for CopperRecordUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. A minimal FakeCopperResource stands
in for the real CopperResource -- it implements only `.upsert()` (the one
external-API boundary this sink calls), backed by an in-memory dict
keyed by the search filter's dedupe value so tests can exercise real
create-vs-update branching without any network. Everything else this
component owns -- dual source resolution, validation, body/filter
shaping per object_type, row iteration, metadata counting -- is
exercised for real, matching the repo's "mock only the external call"
convention (see assets/reverse_etl/google_ads_customer_match_upsert/tests).
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("copper_record_upsert_component", component_py)
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


def _dedupe_key(search_filter: dict):
    """Extract a hashable dedupe key from whatever shape the component built
    (array-wrapped, bare string, or flat passthrough -- all three shapes
    this component can produce)."""
    for v in search_filter.values():
        if isinstance(v, list):
            return v[0] if v else None
        return v
    return None


class FakeCopperResource:
    """Stands in for the real CopperResource -- `.upsert()` only, backed by
    an in-memory store keyed by whatever value the search filter carries.
    Records every call so tests can assert on exact search_filter /
    create_body shapes the component built per object_type."""

    def __init__(self):
        self._store: dict = {}
        self._next_id = 1
        self.upsert_calls = []

    def upsert(self, object_type, search_filter, create_body, update_body=None):
        self.upsert_calls.append(
            {"object_type": object_type, "search_filter": dict(search_filter), "create_body": dict(create_body)}
        )
        key = (object_type, _dedupe_key(search_filter))
        if key in self._store:
            existing_id = self._store[key]
            return {"action": "updated", "id": existing_id}
        new_id = self._next_id
        self._next_id += 1
        self._store[key] = new_id
        return {"action": "created", "id": new_id}
