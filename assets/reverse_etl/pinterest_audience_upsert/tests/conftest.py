"""Shared test helpers for PinterestAudienceUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real network/`requests` calls are
ever made -- `_call_pinterest_update` (the one external, paid-API boundary)
is monkeypatched wholesale in tests, while everything this component
actually owns (dual source resolution, hashing, row-to-record building,
validation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "pinterest_audience_upsert_component", component_py
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


class FakePinterestAdsResource:
    """Stands in for the real PinterestAdsResource -- `_call_pinterest_update`
    is monkeypatched wholesale in tests, so this object's attributes just
    need to exist, never actually get used over the network."""

    def __init__(self, ad_account_id: str = "549756359984"):
        self.ad_account_id = ad_account_id
        self.api_base_url = "https://api.pinterest.com/v5"

    def get_headers(self):
        return {"Authorization": "Bearer fake-token", "Content-Type": "application/json"}


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
