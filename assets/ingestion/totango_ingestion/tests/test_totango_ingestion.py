"""Committed tests for TotangoAccountsIngestionComponent.

Mocks ONLY the Totango HTTP call (via a fake resource object exposing
`.post(path, json)`, matching TotangoResource's real interface) -- every
other behavior (partitions_def construction, DataFrame building, preview
metadata, offset-as-page-number pagination loop logic, terms/range-filter
construction) is exercised for real through dg.materialize().
"""
from typing import Any, Dict, List, Optional

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeTotangoResource:
    """Returns one queued page of hits per `.post()` call, wrapped in
    Totango's real (deeply nested) response shape."""

    def __init__(self, pages: Optional[List[List[Dict[str, Any]]]] = None):
        self.pages = list(pages) if pages is not None else []
        self.calls: List[Dict[str, Any]] = []

    def post(self, path: str, json: Optional[Dict[str, Any]] = None) -> dict:
        self.calls.append({"path": path, "json": json})
        hits = self.pages.pop(0) if self.pages else []
        return {
            "response": {
                "accounts": {"hits": hits, "total_hits": len(hits)},
                "status": {"code": 0, "message": "OK"},
            }
        }


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="totango_accounts", resource_name="totango_resource")
    kwargs.update(overrides)
    return mod.TotangoAccountsIngestionComponent(**kwargs)


def _materialize(component, fake_resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_name: fake_resource},
        partition_key=partition_key,
    )


def test_basic_fetch_single_short_page(mod):
    fake = FakeTotangoResource(pages=[[{"name": "acct1", "display_name": "Acme"}]])
    component = _make_component(mod, count=50, limit=50)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("totango_accounts"))
    assert len(df) == 1
    assert df.iloc[0]["display_name"] == "Acme"
    assert len(fake.calls) == 1  # short page (1 < 50) stops after first call


def test_pagination_increments_offset_as_page_number_not_record_count(mod):
    fake = FakeTotangoResource(pages=[
        [{"name": "a1"}, {"name": "a2"}],
        [{"name": "a3"}, {"name": "a4"}],
        [{"name": "a5"}],
    ])
    component = _make_component(mod, count=2, limit=100)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("totango_accounts"))
    assert len(df) == 5
    assert len(fake.calls) == 3
    # offset is a PAGE NUMBER (0, 1, 2, ...), not a cumulative record count
    assert fake.calls[0]["json"]["offset"] == 0
    assert fake.calls[1]["json"]["offset"] == 1
    assert fake.calls[2]["json"]["offset"] == 2


def test_empty_result_returns_empty_dataframe(mod):
    fake = FakeTotangoResource(pages=[[]])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("totango_accounts"))
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["row_count"].value == 0


def test_limit_truncates_across_pages(mod):
    fake = FakeTotangoResource(pages=[
        [{"name": "a1"}, {"name": "a2"}],
        [{"name": "a3"}, {"name": "a4"}],
    ])
    component = _make_component(mod, count=2, limit=3)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("totango_accounts"))
    assert len(df) == 3


def test_fields_passed_through_to_request_body(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1", "health": "green"}]])
    component = _make_component(mod, fields=["name", "health"])
    result = _materialize(component, fake)
    assert result.success
    assert fake.calls[0]["json"]["fields"] == ["name", "health"]


def test_time_based_partition_builds_range_term(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(
        mod,
        partition_type="daily",
        partition_start="2024-01-01",
        date_term_name="last_updated",
    )
    result = _materialize(component, fake, partition_key="2024-01-02")
    assert result.success
    terms = fake.calls[0]["json"]["terms"]
    range_terms = [t for t in terms if t.get("type") == "range"]
    assert len(range_terms) == 1
    assert range_terms[0]["term"] == "last_updated"
    assert range_terms[0]["params"]["gte"].startswith("2024-01-02")
    assert range_terms[0]["params"]["lte"].startswith("2024-01-03")


def test_static_modified_after_before_builds_range_term(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(
        mod,
        modified_after="2026-01-01T00:00:00Z",
        modified_before="2026-02-01T00:00:00Z",
    )
    result = _materialize(component, fake)
    assert result.success
    terms = fake.calls[0]["json"]["terms"]
    assert terms[0]["params"] == {"gte": "2026-01-01T00:00:00Z", "lte": "2026-02-01T00:00:00Z"}


def test_base_terms_preserved_alongside_date_filter(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(
        mod,
        terms=[{"type": "attribute", "term": "status", "query": "active"}],
        modified_after="2026-01-01T00:00:00Z",
    )
    result = _materialize(component, fake)
    assert result.success
    terms = fake.calls[0]["json"]["terms"]
    assert {"type": "attribute", "term": "status", "query": "active"} in terms
    assert any(t.get("type") == "range" for t in terms)


def test_sort_and_scope_passed_through_when_set(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(mod, sort_by="health", sort_order="desc", scope="my-scope")
    result = _materialize(component, fake)
    assert result.success
    body = fake.calls[0]["json"]
    assert body["sort_by"] == "health"
    assert body["sort_order"] == "desc"
    assert body["scope"] == "my-scope"


def test_sort_and_scope_omitted_when_unset(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    body = fake.calls[0]["json"]
    assert "sort_by" not in body
    assert "sort_order" not in body
    assert "scope" not in body


def test_preview_metadata_included_when_enabled(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1", "display_name": "Acme"}]])
    component = _make_component(mod, include_preview_metadata=True)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" in meta
    assert "Acme" in meta["preview"].value


def test_preview_metadata_excluded_when_disabled(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1", "display_name": "Acme"}]])
    component = _make_component(mod, include_preview_metadata=False)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" not in meta


def test_custom_resource_name(mod):
    fake = FakeTotangoResource(pages=[[{"name": "a1"}]])
    component = _make_component(mod, resource_name="my_totango")
    result = _materialize(component, fake)
    assert result.success
