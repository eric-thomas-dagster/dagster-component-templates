"""Shared test helpers for XeroContactUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The two external-API boundaries
(`_xero_lookup_contact` -- GET /Contacts?where=..., and
`_xero_write_contact` -- POST /Contacts) are monkeypatched wholesale in
tests, while everything this component actually owns (dual source
resolution, fields_map application, lookup validation, the
lookup-then-create-or-update-with-ContactID branching, validation,
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
        "xero_contact_upsert_component", component_py
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


class FakeXeroResource:
    """Stands in for the real XeroResource -- `.request()` is never
    actually invoked since the lookup/write functions are monkeypatched
    wholesale in tests."""

    def request(self, method, path, json_body=None, params=None):
        raise AssertionError("FakeXeroResource.request() should never be called directly in tests")


def metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
