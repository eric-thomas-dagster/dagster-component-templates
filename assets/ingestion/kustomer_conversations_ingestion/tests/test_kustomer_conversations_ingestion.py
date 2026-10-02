"""Committed tests for KustomerConversationsIngestionComponent.

Mocks only the HTTP call (via FakeKustomerResource). Everything else -- the
page-walking loop (integer page/pageSize, not a cursor), the
conversation_created_at filter construction, JSON:API attribute flattening,
DataFrame building, preview metadata, and partitions_def construction --
runs for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeKustomerResource, load_component_module, make_conversation_object


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource, **kwargs):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={component.resource_name: resource}, **kwargs)


def test_single_page_materializes_dataframe_with_flattened_attributes(mod):
    resource = FakeKustomerResource([
        {"data": [
            make_conversation_object("c1", "2026-06-05T00:00:00.000Z"),
            make_conversation_object("c2", "2026-06-06T00:00:00.000Z"),
        ]},
    ])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("kustomer_conversations")
    assert len(df) == 2
    assert set(df["id"]) == {"c1", "c2"}
    assert "conversation_created_at" in df.columns
    assert df.loc[df["id"] == "c1", "conversation_status"].iloc[0] == "open"


def test_search_request_uses_queryContext_conversation(mod):
    resource = FakeKustomerResource([{"data": []}])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    _materialize(component, resource)
    assert resource.calls[0]["json"]["queryContext"] == "conversation"
    assert resource.calls[0]["path"] == "v1/customers/search"


def test_date_window_builds_conversation_created_at_and_filter(mod):
    resource = FakeKustomerResource([{"data": []}])
    component = mod.KustomerConversationsIngestionComponent(
        asset_name="kustomer_conversations",
        from_date_time="2026-06-01T00:00:00.000Z",
        to_date_time="2026-07-01T00:00:00.000Z",
    )
    _materialize(component, resource)
    and_filter = resource.calls[0]["json"]["and"]
    assert and_filter == [{"conversation_created_at": {
        "gte": "2026-06-01T00:00:00.000Z", "lt": "2026-07-01T00:00:00.000Z",
    }}]


def test_no_date_window_sends_no_and_filter(mod):
    resource = FakeKustomerResource([{"data": []}])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    _materialize(component, resource)
    assert "and" not in resource.calls[0]["json"]


def test_pagination_walks_page_number_across_multiple_pages(mod):
    resource = FakeKustomerResource([
        {"data": [make_conversation_object(f"c{i}", "2026-06-01T00:00:00.000Z") for i in range(100)]},
        {"data": [make_conversation_object(f"c{100+i}", "2026-06-01T00:00:00.000Z") for i in range(50)]},
    ])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations", limit=150)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("kustomer_conversations")
    assert len(df) == 150
    assert resource.calls[0]["json"]["page"] == 1
    assert resource.calls[1]["json"]["page"] == 2


def test_pagination_stops_when_a_page_comes_back_empty(mod):
    resource = FakeKustomerResource([
        {"data": [make_conversation_object("c1", "2026-06-01T00:00:00.000Z")]},
        {"data": []},
    ])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations", limit=1000)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("kustomer_conversations")
    assert len(df) == 1
    assert len(resource.calls) == 2


def test_pagination_stops_when_limit_reached_before_pages_exhausted(mod):
    resource = FakeKustomerResource([
        {"data": [make_conversation_object(f"c{i}", "2026-06-01T00:00:00.000Z") for i in range(100)]},
        {"data": [make_conversation_object(f"c{100+i}", "2026-06-01T00:00:00.000Z") for i in range(100)]},
    ])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations", limit=120)
    result = _materialize(component, resource)
    df = result.output_for_node("kustomer_conversations")
    assert len(df) == 120
    assert len(resource.calls) == 2


def test_empty_results_returns_empty_dataframe_with_zero_row_count(mod):
    resource = FakeKustomerResource([{"data": []}])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("kustomer_conversations")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    mats = result.asset_materializations_for_node("kustomer_conversations")
    assert mats[-1].metadata["row_count"].value == 0


def test_max_pages_safety_cap_is_respected(mod):
    # Every page returns a full page, so without max_pages this would loop
    # until `limit` -- verify the cap actually bounds the number of requests.
    infinite_pages = [
        {"data": [make_conversation_object(f"p{p}-{i}", "2026-06-01T00:00:00.000Z") for i in range(100)]}
        for p in range(10)
    ]
    resource = FakeKustomerResource(infinite_pages)
    component = mod.KustomerConversationsIngestionComponent(
        asset_name="kustomer_conversations", limit=100000, max_pages=3
    )
    result = _materialize(component, resource)
    assert result.success
    assert len(resource.calls) == 3


def test_preview_metadata_included_by_default(mod):
    resource = FakeKustomerResource([{"data": [make_conversation_object("c1", "2026-06-01T00:00:00.000Z")]}])
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    result = _materialize(component, resource)
    mats = result.asset_materializations_for_node("kustomer_conversations")
    assert "preview" in mats[-1].metadata


def test_default_kinds_and_group_name(mod):
    component = mod.KustomerConversationsIngestionComponent(asset_name="kustomer_conversations")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec()
    assert spec.group_name == "kustomer"
    assert "dagster/kind/kustomer" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_custom_resource_name_is_required(mod):
    resource = FakeKustomerResource([{"data": [make_conversation_object("c1", "2026-06-01T00:00:00.000Z")]}])
    component = mod.KustomerConversationsIngestionComponent(
        asset_name="kustomer_conversations", resource_name="custom_kustomer"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"custom_kustomer": resource})
    assert result.success


def test_daily_partitions_def_derives_window_from_partition_key(mod):
    resource = FakeKustomerResource([{"data": [make_conversation_object("c1", "2026-06-02T00:00:00.000Z")]}])
    component = mod.KustomerConversationsIngestionComponent(
        asset_name="kustomer_conversations",
        partition_type="daily",
        partition_start="2026-06-01",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def], resources={"kustomer_resource": resource}, partition_key="2026-06-02"
    )
    assert result.success
    mats = result.asset_materializations_for_node("kustomer_conversations")
    assert mats[-1].metadata["from_date_time"].value == "2026-06-02T00:00:00.000Z"
    assert mats[-1].metadata["to_date_time"].value == "2026-06-03T00:00:00.000Z"


def test_bad_partition_config_raises_value_error(mod):
    component = mod.KustomerConversationsIngestionComponent(
        asset_name="kustomer_conversations", partition_type="daily"
    )
    with pytest.raises(ValueError, match="requires partition_start"):
        component.build_defs(context=None)
