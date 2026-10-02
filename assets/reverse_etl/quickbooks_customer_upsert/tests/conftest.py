"""Shared test helpers for QuickBooksCustomerUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The two external-API boundaries
(`_quickbooks_query_customer` -- GET /query, and
`_quickbooks_write_customer` -- POST /customer) are monkeypatched
wholesale in tests, while everything this component actually owns (dual
source resolution, fields_map application incl. dotted-key nesting,
lookup validation, the query-then-create-or-update-with-SyncToken
branching, validation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "quickbooks_customer_upsert_component", component_py
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


class FakeQuickBooksResource:
    """Stands in for the real QuickBooksResource -- `.query()` /
    `.request()` are never actually invoked since the query/write
    functions are monkeypatched wholesale in tests."""

    def query(self, query_str):
        raise AssertionError("FakeQuickBooksResource.query() should never be called directly in tests")

    def request(self, method, path, json_body=None, params=None):
        raise AssertionError("FakeQuickBooksResource.request() should never be called directly in tests")


def metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
