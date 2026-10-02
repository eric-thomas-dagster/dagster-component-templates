"""Committed tests for VitallyAccountsIngestionComponent.

Mocks ONLY the Vitally HTTP call (via a fake resource object exposing
`.get(path, params)`, matching VitallyResource's real interface) -- every
other behavior (partitions_def construction, DataFrame building, preview
metadata, cursor-pagination loop logic, error handling) is exercised for
real through dg.materialize().
"""
from typing import Any, Dict, List, Optional

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeVitallyResource:
    """Returns one queued (page, next_cursor) pair per `.get()` call."""

    def __init__(self, pages: Optional[List[tuple]] = None):
        # each entry: (list_of_results, next_cursor_or_None)
        self.pages = list(pages) if pages is not None else []
        self.calls: List[Dict[str, Any]] = []

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        self.calls.append({"path": path, "params": params})
        if self.pages:
            results, next_cursor = self.pages.pop(0)
        else:
            results, next_cursor = [], None
        return {"results": results, "next": next_cursor}


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="vitally_accounts", resource_name="vitally_resource")
    kwargs.update(overrides)
    return mod.VitallyAccountsIngestionComponent(**kwargs)


def _materialize(component, fake_resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_name: fake_resource},
        partition_key=partition_key,
    )


def test_basic_fetch_single_page_no_cursor(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1", "name": "Acme"}], None)])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("vitally_accounts"))
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Acme"
    assert len(fake.calls) == 1


def test_pagination_follows_cursor_across_pages(mod):
    fake = FakeVitallyResource(pages=[
        ([{"id": "a1"}, {"id": "a2"}], "cursor-1"),
        ([{"id": "a3"}, {"id": "a4"}], "cursor-2"),
        ([{"id": "a5"}], None),
    ])
    component = _make_component(mod, limit=100, page_size=2)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("vitally_accounts"))
    assert len(df) == 5
    assert list(df["id"]) == ["a1", "a2", "a3", "a4", "a5"]
    assert len(fake.calls) == 3
    assert "from" not in fake.calls[0]["params"]
    assert fake.calls[1]["params"]["from"] == "cursor-1"
    assert fake.calls[2]["params"]["from"] == "cursor-2"


def test_pagination_stops_when_cursor_present_but_page_empty(mod):
    """Defensive: even if the API returns a non-null cursor alongside an
    empty page (shouldn't happen, but must not infinite-loop), the fetch
    loop stops."""
    fake = FakeVitallyResource(pages=[([], "cursor-should-be-ignored")])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    assert len(fake.calls) == 1


def test_empty_result_returns_empty_dataframe(mod):
    fake = FakeVitallyResource(pages=[([], None)])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("vitally_accounts"))
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["row_count"].value == 0


def test_limit_truncates_across_pages(mod):
    fake = FakeVitallyResource(pages=[
        ([{"id": "a1"}, {"id": "a2"}], "cursor-1"),
        ([{"id": "a3"}, {"id": "a4"}], "cursor-2"),
    ])
    component = _make_component(mod, limit=3, page_size=2)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("vitally_accounts"))
    assert len(df) == 3


def test_status_filter_passed_through(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1"}], None)])
    component = _make_component(mod, status="churned")
    result = _materialize(component, fake)
    assert result.success
    assert fake.calls[0]["params"]["status"] == "churned"


def test_sort_by_passed_through(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1"}], None)])
    component = _make_component(mod, sort_by="createdAt")
    result = _materialize(component, fake)
    assert result.success
    assert fake.calls[0]["params"]["sortBy"] == "createdAt"


def test_time_based_partition_does_not_filter_but_tags_metadata(mod):
    """Vitally's Accounts endpoint has no server-side date filter -- a
    time-based partition should still resolve cleanly and tag metadata,
    but must NOT appear as a query param (there is nowhere to put it)."""
    fake = FakeVitallyResource(pages=[([{"id": "a1"}], None)])
    component = _make_component(mod, partition_type="daily", partition_start="2024-01-01")
    result = _materialize(component, fake, partition_key="2024-01-02")
    assert result.success
    params = fake.calls[0]["params"]
    assert "from" not in params or params.get("from") != "2024-01-02"
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["partition_window_start"].value.startswith("2024-01-02")
    assert meta["partition_window_end"].value.startswith("2024-01-03")


def test_preview_metadata_included_when_enabled(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1", "name": "Acme"}], None)])
    component = _make_component(mod, include_preview_metadata=True)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" in meta
    assert "Acme" in meta["preview"].value


def test_preview_metadata_excluded_when_disabled(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1", "name": "Acme"}], None)])
    component = _make_component(mod, include_preview_metadata=False)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" not in meta


def test_custom_resource_name(mod):
    fake = FakeVitallyResource(pages=[([{"id": "a1"}], None)])
    component = _make_component(mod, resource_name="my_vitally")
    result = _materialize(component, fake)
    assert result.success
