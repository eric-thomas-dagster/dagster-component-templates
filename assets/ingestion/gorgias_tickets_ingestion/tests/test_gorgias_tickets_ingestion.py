"""Committed tests for GorgiasTicketsIngestionComponent.

Mocks only the HTTP call (via FakeGorgiasResource). Everything else -- the
cursor-walking pagination loop, the client-side date-window filter (Gorgias
has no server-side one), DataFrame construction, preview metadata, and
partitions_def construction -- runs for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeGorgiasResource, load_component_module, make_ticket


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={component.resource_name: resource})


def test_single_page_materializes_dataframe(mod):
    resource = FakeGorgiasResource([
        {"data": [make_ticket(1, "2026-06-05T00:00:00Z"), make_ticket(2, "2026-06-06T00:00:00Z")], "meta": {}},
    ])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("gorgias_tickets")
    assert len(df) == 2
    assert set(df["id"]) == {1, 2}


def test_pagination_walks_cursor_across_multiple_pages(mod):
    resource = FakeGorgiasResource([
        {"data": [make_ticket(i, "2026-06-01T00:00:00Z") for i in range(1, 101)], "meta": {"next_cursor": "abc"}},
        {"data": [make_ticket(i, "2026-06-01T00:00:00Z") for i in range(101, 151)], "meta": {}},
    ])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets", limit=150)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("gorgias_tickets")
    assert len(df) == 150
    # second request must have carried the cursor forward
    assert resource.calls[1]["params"]["cursor"] == "abc"
    assert "cursor" not in resource.calls[0]["params"]


def test_pagination_stops_when_limit_reached_before_cursor_exhausted(mod):
    resource = FakeGorgiasResource([
        {"data": [make_ticket(i, "2026-06-01T00:00:00Z") for i in range(1, 101)], "meta": {"next_cursor": "abc"}},
        {"data": [make_ticket(i, "2026-06-01T00:00:00Z") for i in range(101, 201)], "meta": {"next_cursor": "def"}},
    ])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets", limit=120)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("gorgias_tickets")
    assert len(df) == 120
    # only 2 requests needed even though a 3rd cursor was available
    assert len(resource.calls) == 2


def test_empty_results_returns_empty_dataframe_with_zero_row_count(mod):
    resource = FakeGorgiasResource([{"data": [], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("gorgias_tickets")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    mats = result.asset_materializations_for_node("gorgias_tickets")
    assert mats[-1].metadata["row_count"].value == 0


def test_client_side_date_window_filters_out_of_range_tickets(mod):
    resource = FakeGorgiasResource([
        {"data": [
            make_ticket(1, "2026-05-30T00:00:00Z"),  # before window
            make_ticket(2, "2026-06-05T00:00:00Z"),  # in window
            make_ticket(3, "2026-07-02T00:00:00Z"),  # after window
        ], "meta": {}},
    ])
    component = mod.GorgiasTicketsIngestionComponent(
        asset_name="gorgias_tickets",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("gorgias_tickets")
    assert list(df["id"]) == [2]


def test_order_by_param_is_passed_through(mod):
    resource = FakeGorgiasResource([{"data": [make_ticket(1, "2026-06-01T00:00:00Z")], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets", order_by="updated_datetime:asc")
    result = _materialize(component, resource)
    assert result.success
    assert resource.calls[0]["params"]["order_by"] == "updated_datetime:asc"


def test_page_size_above_100_is_rejected_by_field_validation(mod):
    # Gorgias caps page size at 100 server-side; the field itself enforces
    # that (le=100) rather than silently clamping an out-of-range config.
    with pytest.raises(Exception):
        mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets", page_size=500)


def test_page_size_default_is_sent_as_limit_param(mod):
    resource = FakeGorgiasResource([{"data": [], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets")
    _materialize(component, resource)
    assert resource.calls[0]["params"]["limit"] == 100


def test_preview_metadata_included_by_default(mod):
    resource = FakeGorgiasResource([{"data": [make_ticket(1, "2026-06-01T00:00:00Z")], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets")
    result = _materialize(component, resource)
    mats = result.asset_materializations_for_node("gorgias_tickets")
    assert "preview" in mats[-1].metadata


def test_preview_metadata_can_be_disabled(mod):
    resource = FakeGorgiasResource([{"data": [make_ticket(1, "2026-06-01T00:00:00Z")], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets", include_preview_metadata=False)
    result = _materialize(component, resource)
    mats = result.asset_materializations_for_node("gorgias_tickets")
    assert "preview" not in mats[-1].metadata


def test_default_kinds_and_group_name(mod):
    component = mod.GorgiasTicketsIngestionComponent(asset_name="gorgias_tickets")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec()
    assert spec.group_name == "gorgias"
    assert "dagster/kind/gorgias" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_custom_resource_name_is_required(mod):
    resource = FakeGorgiasResource([{"data": [make_ticket(1, "2026-06-01T00:00:00Z")], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(
        asset_name="gorgias_tickets", resource_name="custom_gorgias"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"custom_gorgias": resource})
    assert result.success


def test_daily_partitions_def_derives_window_from_partition_key(mod):
    resource = FakeGorgiasResource([{"data": [make_ticket(1, "2026-06-01T00:00:00Z")], "meta": {}}])
    component = mod.GorgiasTicketsIngestionComponent(
        asset_name="gorgias_tickets",
        partition_type="daily",
        partition_start="2026-06-01",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def], resources={"gorgias_resource": resource}, partition_key="2026-06-02"
    )
    assert result.success
    mats = result.asset_materializations_for_node("gorgias_tickets")
    assert mats[-1].metadata["from_date_time"].value == "2026-06-02T00:00:00Z"
    assert mats[-1].metadata["to_date_time"].value == "2026-06-03T00:00:00Z"


def test_bad_partition_config_raises_value_error(mod):
    component = mod.GorgiasTicketsIngestionComponent(
        asset_name="gorgias_tickets", partition_type="daily"
    )
    with pytest.raises(ValueError, match="requires partition_start"):
        component.build_defs(context=None)
