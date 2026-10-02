"""Shared test helpers for TikTokCustomAudienceUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real network/`requests` calls are
ever made -- `_call_tiktok_upload` / `_call_tiktok_update` (the two
external, paid-API boundaries) are monkeypatched wholesale in tests, while
everything this component actually owns (dual source resolution, hashing,
per-type file grouping, validation, metadata) is exercised for real.
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
        "tiktok_custom_audience_upsert_component", component_py
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


class FakeTikTokAdsResource:
    """Stands in for the real TikTokAdsResource -- `_call_tiktok_upload` /
    `_call_tiktok_update` are monkeypatched wholesale in tests, so this
    object's attributes just need to exist, never actually get used over
    the network."""

    def __init__(self, advertiser_id: str = "1234567890123456789"):
        self.advertiser_id = advertiser_id
        self.api_base_url = "https://business-api.tiktok.com/open_api/v1.3"

    def get_headers(self):
        return {"Access-Token": "fake-token"}


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
