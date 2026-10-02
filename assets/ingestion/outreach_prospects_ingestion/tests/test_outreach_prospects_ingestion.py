"""Committed regression tests for OutreachProspectsIngestionComponent.

The real Outreach REST API is never called -- `FakeOutreachResource`
(conftest.py) stands in for the one external, paid-API boundary
(`resource.get(path, params)`), while everything this component actually
owns -- JSON:API attribute flattening, cursor-following pagination,
partitions_def construction, filter-param translation, limit enforcement,
and preview metadata -- is exercised for real via `dg.materialize`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeOutreachResource, load_component_module, make_prospect


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={"outreach_resource": resource},
        partition_key=partition_key,
    )


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- JSON:API flattening (pure, no resource involved) --------------------

def test_flatten_json_api_spreads_attributes_to_columns(mod):
    records = [make_prospect("1", firstName="Jane", lastName="Doe", emails=["jane@example.com"])]
    df = mod._flatten_json_api(records)
    assert list(df.columns) == ["id", "type", "firstName", "lastName", "emails"]
    assert df.iloc[0]["firstName"] == "Jane"
    assert df.iloc[0]["type"] == "prospect"


def test_flatten_json_api_drops_relationships(mod):
    record = {"id": "1", "type": "prospect", "attributes": {"firstName": "Jane"}, "relationships": {"account": {"data": {"id": "9"}}}}
    df = mod._flatten_json_api([record])
    assert "relationships" not in df.columns
    assert "account" not in df.columns


def test_flatten_json_api_handles_missing_attributes(mod):
    record = {"id": "1", "type": "prospect"}
    df = mod._flatten_json_api([record])
    assert df.iloc[0]["id"] == "1"
    assert df.iloc[0]["type"] == "prospect"


# --- partitions_def construction -----------------------------------------

def test_no_partition_type_means_unpartitioned(mod):
    component = mod.OutreachProspectsIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert asset_def.partitions_def is None


def test_daily_partition_type_builds_daily_partitions_def(mod):
    component = mod.OutreachProspectsIngestionComponent(
        asset_name="x", partition_type="daily", partition_start="2026-01-01"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert isinstance(asset_def.partitions_def, dg.DailyPartitionsDefinition)


def test_daily_partition_without_start_raises(mod):
    component = mod.OutreachProspectsIngestionComponent(asset_name="x", partition_type="daily")
    with pytest.raises(ValueError, match="requires partition_start"):
        component.build_defs(context=None)


# --- end-to-end against the fake resource --------------------------------

def test_single_page_materializes_full_dataframe(mod):
    page = {
        "data": [
            make_prospect("1", firstName="Jane", lastName="Doe"),
            make_prospect("2", firstName="Bob", lastName="Smith"),
        ],
        "links": {},
    }
    resource = FakeOutreachResource([page])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "outreach_prospects")
    assert out["row_count"] == 2
    # Only one call was needed -- no links.next present.
    assert len(resource.calls) == 1
    assert resource.calls[0]["path"] == "prospects"


def test_pagination_follows_links_next_until_absent(mod):
    page1 = {"data": [make_prospect("1", firstName="A")], "links": {"next": "https://api.outreach.io/api/v2/prospects?page%5Bcursor%5D=abc"}}
    page2 = {"data": [make_prospect("2", firstName="B")], "links": {}}
    resource = FakeOutreachResource([page1, page2])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects", limit=100)
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "outreach_prospects")
    assert out["row_count"] == 2
    assert len(resource.calls) == 2
    # Second call passed the full links.next URL, not a relative path + params.
    assert resource.calls[1]["path"].startswith("https://api.outreach.io")
    assert resource.calls[1]["params"] is None


def test_limit_stops_pagination_early(mod):
    page1 = {
        "data": [make_prospect("1", firstName="A"), make_prospect("2", firstName="B")],
        "links": {"next": "https://api.outreach.io/api/v2/prospects?page%5Bcursor%5D=abc"},
    }
    page2 = {"data": [make_prospect("3", firstName="C")], "links": {}}
    resource = FakeOutreachResource([page1, page2])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects", limit=2)
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "outreach_prospects")
    # Limit satisfied by page 1 alone -- page 2 is never fetched.
    assert out["row_count"] == 2
    assert len(resource.calls) == 1


def test_empty_result_returns_empty_dataframe_and_zero_row_count(mod):
    resource = FakeOutreachResource([{"data": [], "links": {}}])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "outreach_prospects")
    assert out["row_count"] == 0
    assert "preview" not in out


def test_preview_metadata_included_when_enabled(mod):
    page = {"data": [make_prospect("1", firstName="Jane")], "links": {}}
    resource = FakeOutreachResource([page])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects", include_preview_metadata=True)
    result = _materialize(component, resource)
    out = _metadata_for(result, "outreach_prospects")
    assert "preview" in out


def test_preview_metadata_omitted_when_disabled(mod):
    page = {"data": [make_prospect("1", firstName="Jane")], "links": {}}
    resource = FakeOutreachResource([page])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects", include_preview_metadata=False)
    result = _materialize(component, resource)
    out = _metadata_for(result, "outreach_prospects")
    assert "preview" not in out


def test_updated_after_and_before_translate_to_gt_lt_filter_params(mod):
    resource = FakeOutreachResource([{"data": [], "links": {}}])
    component = mod.OutreachProspectsIngestionComponent(
        asset_name="outreach_prospects",
        updated_after="2026-06-01T00:00:00Z",
        updated_before="2026-07-01T00:00:00Z",
    )
    _materialize(component, resource)
    params = resource.calls[0]["params"]
    assert params["filter[updatedAt][gt]"] == "2026-06-01T00:00:00Z"
    assert params["filter[updatedAt][lt]"] == "2026-07-01T00:00:00Z"


def test_no_time_filter_omits_filter_params(mod):
    resource = FakeOutreachResource([{"data": [], "links": {}}])
    component = mod.OutreachProspectsIngestionComponent(asset_name="outreach_prospects")
    _materialize(component, resource)
    params = resource.calls[0]["params"]
    assert not any(k.startswith("filter[updatedAt]") for k in params)


def test_time_partition_window_overrides_static_config(mod):
    resource = FakeOutreachResource([{"data": [], "links": {}}])
    component = mod.OutreachProspectsIngestionComponent(
        asset_name="outreach_prospects",
        updated_after="2000-01-01T00:00:00Z",  # should be ignored in favor of the partition window
        updated_before="2000-01-02T00:00:00Z",
        partition_type="daily",
        partition_start="2026-01-01",
    )
    _materialize(component, resource, partition_key="2026-01-05")
    params = resource.calls[0]["params"]
    assert params["filter[updatedAt][gt]"] == "2026-01-05T00:00:00Z"
    assert params["filter[updatedAt][lt]"] == "2026-01-06T00:00:00Z"


def test_default_kinds_tag_outreach_and_python(mod):
    component = mod.OutreachProspectsIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(dg.AssetKey("x"))
    assert "dagster/kind/outreach" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_required_resource_keys_includes_configured_resource_name(mod):
    component = mod.OutreachProspectsIngestionComponent(asset_name="x", resource_name="my_outreach")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert "my_outreach" in asset_def.required_resource_keys
