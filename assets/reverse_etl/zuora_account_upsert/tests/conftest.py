"""Shared test helpers for ZuoraAccountUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The two external-API boundaries
(`_zuora_lookup_account` -- ZOQL query via POST /v1/action/query, and
`_zuora_write_account` -- PUT to update / POST to create) are
monkeypatched wholesale in tests, while everything this component
actually owns (dual source resolution, fields_map application, lookup
validation, the query-then-create-or-update branching, validation,
metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "zuora_account_upsert_component", component_py
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


class FakeZuoraResource:
    """Stands in for the real ZuoraResource -- `.query()` / `.request()`
    are never actually invoked since the lookup/write functions are
    monkeypatched wholesale in tests."""

    def query(self, zoql):
        raise AssertionError("FakeZuoraResource.query() should never be called directly in tests")

    def request(self, method, path, json_body=None, params=None):
        raise AssertionError("FakeZuoraResource.request() should never be called directly in tests")


def metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
