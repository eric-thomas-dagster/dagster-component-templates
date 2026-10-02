"""Committed tests for ChurnZeroAccountsIngestionComponent.

Mocks ONLY the ChurnZero HTTP calls (via a fake resource object exposing
`.get(path, params)` and `.get_url(full_url)`, matching ChurnZeroResource's
real interface) -- every other behavior (partitions_def construction,
DataFrame building, preview metadata, OData nextLink pagination, $filter
construction) is exercised for real through dg.materialize().
"""
from typing import Any, Dict, List, Optional

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeChurnZeroResource:
    """Simulates OData responses: first call via `.get()`, subsequent calls
    (when a response carries '@odata.nextLink') via `.get_url()`."""

    def __init__(self, pages: Optional[List[tuple]] = None):
        # each entry: (list_of_values, next_link_or_None)
        self.pages = list(pages) if pages is not None else []
        self.get_calls: List[Dict[str, Any]] = []
        self.get_url_calls: List[str] = []

    def _next_page(self) -> dict:
        if self.pages:
            values, next_link = self.pages.pop(0)
        else:
            values, next_link = [], None
        body = {"value": values}
        if next_link:
            body["@odata.nextLink"] = next_link
        return body

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        self.get_calls.append({"path": path, "params": params})
        return self._next_page()

    def get_url(self, full_url: str) -> dict:
        self.get_url_calls.append(full_url)
        return self._next_page()


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="churnzero_accounts", resource_name="churnzero_resource")
    kwargs.update(overrides)
    return mod.ChurnZeroAccountsIngestionComponent(**kwargs)


def _materialize(component, fake_resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_name: fake_resource},
        partition_key=partition_key,
    )


def test_basic_fetch_single_page_no_next_link(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1", "Name": "Acme"}], None)])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("churnzero_accounts"))
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "Acme"
    assert len(fake.get_calls) == 1
    assert len(fake.get_url_calls) == 0


def test_pagination_follows_odata_next_link(mod):
    fake = FakeChurnZeroResource(pages=[
        ([{"Id": "1"}, {"Id": "2"}], "https://tenant.churnzero.net/public/v1/Account?$skiptoken=abc"),
        ([{"Id": "3"}], None),
    ])
    component = _make_component(mod, limit=100, top=2)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("churnzero_accounts"))
    assert len(df) == 3
    assert len(fake.get_calls) == 1
    assert len(fake.get_url_calls) == 1
    assert fake.get_url_calls[0] == "https://tenant.churnzero.net/public/v1/Account?$skiptoken=abc"


def test_empty_result_returns_empty_dataframe(mod):
    fake = FakeChurnZeroResource(pages=[([], None)])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("churnzero_accounts"))
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["row_count"].value == 0


def test_limit_truncates_across_pages(mod):
    fake = FakeChurnZeroResource(pages=[
        ([{"Id": "1"}, {"Id": "2"}], "https://tenant.churnzero.net/next"),
        ([{"Id": "3"}, {"Id": "4"}], None),
    ])
    component = _make_component(mod, limit=3, top=2)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("churnzero_accounts"))
    assert len(df) == 3


def test_top_and_orderby_sent_on_first_request(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1"}], None)])
    component = _make_component(mod, top=25, order_by="Name")
    result = _materialize(component, fake)
    assert result.success
    params = fake.get_calls[0]["params"]
    assert params["$top"] == 25
    assert params["$orderby"] == "Name"


def test_static_filter_expr_sent_as_filter(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1"}], None)])
    component = _make_component(mod, filter_expr="Status eq 'Active'")
    result = _materialize(component, fake)
    assert result.success
    assert fake.get_calls[0]["params"]["$filter"] == "Status eq 'Active'"


def test_time_based_partition_builds_odata_date_filter(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1"}], None)])
    component = _make_component(
        mod,
        partition_type="daily",
        partition_start="2024-01-01",
        date_field="ObjectLastModifiedDate",
    )
    result = _materialize(component, fake, partition_key="2024-01-02")
    assert result.success
    filter_str = fake.get_calls[0]["params"]["$filter"]
    assert "ObjectLastModifiedDate ge 2024-01-02" in filter_str
    assert "ObjectLastModifiedDate lt 2024-01-03" in filter_str


def test_static_filter_and_partition_filter_combined_with_and(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1"}], None)])
    component = _make_component(
        mod,
        filter_expr="Status eq 'Active'",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    result = _materialize(component, fake, partition_key="2024-01-02")
    assert result.success
    filter_str = fake.get_calls[0]["params"]["$filter"]
    assert "Status eq 'Active'" in filter_str
    assert " and " in filter_str


def test_preview_metadata_included_when_enabled(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1", "Name": "Acme"}], None)])
    component = _make_component(mod, include_preview_metadata=True)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" in meta
    assert "Acme" in meta["preview"].value


def test_preview_metadata_excluded_when_disabled(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1", "Name": "Acme"}], None)])
    component = _make_component(mod, include_preview_metadata=False)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" not in meta


def test_custom_resource_name(mod):
    fake = FakeChurnZeroResource(pages=[([{"Id": "1"}], None)])
    component = _make_component(mod, resource_name="my_churnzero")
    result = _materialize(component, fake)
    assert result.success
