"""Committed tests for DriftConversationsIngestionComponent.

Mocks only the HTTP call (via FakeDriftResource). Everything else -- the
page_token-walking pagination loop, the client-side date-window filter
(Drift has no server-side one), per-conversation message fetch + flatten,
DataFrame construction, preview metadata, and partitions_def construction --
runs for real.
"""
import datetime

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeDriftResource, load_component_module, make_conversation


def _epoch_ms(iso_str: str) -> int:
    dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
    return int(dt.timestamp() * 1000)


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource, **kwargs):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={component.resource_name: resource}, **kwargs)


def test_single_page_materializes_dataframe(mod):
    resource = FakeDriftResource(list_pages=[
        {"data": [
            make_conversation(1, _epoch_ms("2026-06-05T00:00:00Z")),
            make_conversation(2, _epoch_ms("2026-06-06T00:00:00Z")),
        ], "pagination": {}},
    ])
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert len(df) == 2
    assert set(df["id"]) == {1, 2}


def test_pagination_walks_page_token_across_multiple_pages(mod):
    resource = FakeDriftResource(list_pages=[
        {"data": [make_conversation(i, _epoch_ms("2026-06-01T00:00:00Z")) for i in range(1, 101)],
         "pagination": {"next": "tok-2"}},
        {"data": [make_conversation(i, _epoch_ms("2026-06-01T00:00:00Z")) for i in range(101, 151)],
         "pagination": {}},
    ])
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", limit=150)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert len(df) == 150
    assert resource.calls[1]["params"]["page_token"] == "tok-2"
    assert "page_token" not in resource.calls[0]["params"]


def test_pagination_stops_when_limit_reached_before_token_exhausted(mod):
    resource = FakeDriftResource(list_pages=[
        {"data": [make_conversation(i, _epoch_ms("2026-06-01T00:00:00Z")) for i in range(1, 101)],
         "pagination": {"next": "tok-2"}},
        {"data": [make_conversation(i, _epoch_ms("2026-06-01T00:00:00Z")) for i in range(101, 201)],
         "pagination": {"next": "tok-3"}},
    ])
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", limit=120)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert len(df) == 120
    assert len(resource.calls) == 2


def test_empty_results_returns_empty_dataframe_with_zero_row_count(mod):
    resource = FakeDriftResource(list_pages=[{"data": [], "pagination": {}}])
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations")
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    mats = result.asset_materializations_for_node("drift_conversations")
    assert mats[-1].metadata["row_count"].value == 0


def test_client_side_date_window_filters_out_of_range_conversations(mod):
    resource = FakeDriftResource(list_pages=[
        {"data": [
            make_conversation(1, _epoch_ms("2026-05-30T00:00:00Z")),  # before window
            make_conversation(2, _epoch_ms("2026-06-05T00:00:00Z")),  # in window
            make_conversation(3, _epoch_ms("2026-07-02T00:00:00Z")),  # after window
        ], "pagination": {}},
    ])
    component = mod.DriftConversationsIngestionComponent(
        asset_name="drift_conversations",
        from_date_time="2026-06-01T00:00:00Z",
        to_date_time="2026-07-01T00:00:00Z",
    )
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert list(df["id"]) == [2]


def test_status_id_param_is_passed_through(mod):
    resource = FakeDriftResource(list_pages=[{"data": [], "pagination": {}}])
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", status_id=2)
    _materialize(component, resource)
    assert resource.calls[0]["params"]["statusId"] == 2


def test_include_messages_flattens_transcript(mod):
    resource = FakeDriftResource(
        list_pages=[{"data": [make_conversation(1, _epoch_ms("2026-06-05T00:00:00Z"))], "pagination": {}}],
        messages_by_conv={1: [
            {"messages": [
                {"author": "visitor", "body": "Hi there"},
                {"author": "agent", "body": "How can I help?"},
            ], "pagination": {}},
        ]},
    )
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", include_messages=True)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert df.loc[0, "transcript"] == "visitor: Hi there\nagent: How can I help?"
    assert len(df.loc[0, "messages_raw"]) == 2


def test_include_messages_paginates_across_message_pages(mod):
    resource = FakeDriftResource(
        list_pages=[{"data": [make_conversation(7, _epoch_ms("2026-06-05T00:00:00Z"))], "pagination": {}}],
        messages_by_conv={7: [
            {"messages": [{"author": "visitor", "body": "part one"}], "pagination": {"next": "mtok-2"}},
            {"messages": [{"author": "agent", "body": "part two"}], "pagination": {}},
        ]},
    )
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", include_messages=True)
    result = _materialize(component, resource)
    assert result.success
    df = result.output_for_node("drift_conversations")
    assert df.loc[0, "transcript"] == "visitor: part one\nagent: part two"
    message_calls = [c for c in resource.calls if c["path"] == "conversations/7/messages"]
    assert len(message_calls) == 2
    assert message_calls[1]["params"]["next"] == "mtok-2"


def test_messages_not_fetched_when_include_messages_false(mod):
    resource = FakeDriftResource(
        list_pages=[{"data": [make_conversation(1, _epoch_ms("2026-06-05T00:00:00Z"))], "pagination": {}}],
    )
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations", include_messages=False)
    result = _materialize(component, resource)
    df = result.output_for_node("drift_conversations")
    assert "transcript" not in df.columns
    assert all(c["path"] == "conversations/list" for c in resource.calls)


def test_default_kinds_and_group_name(mod):
    component = mod.DriftConversationsIngestionComponent(asset_name="drift_conversations")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec()
    assert spec.group_name == "drift"
    assert "dagster/kind/drift" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_custom_resource_name_is_required(mod):
    resource = FakeDriftResource(list_pages=[{"data": [make_conversation(1, _epoch_ms("2026-06-01T00:00:00Z"))], "pagination": {}}])
    component = mod.DriftConversationsIngestionComponent(
        asset_name="drift_conversations", resource_name="custom_drift"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"custom_drift": resource})
    assert result.success


def test_daily_partitions_def_derives_window_from_partition_key(mod):
    resource = FakeDriftResource(list_pages=[
        {"data": [make_conversation(1, _epoch_ms("2026-06-02T12:00:00Z"))], "pagination": {}},
    ])
    component = mod.DriftConversationsIngestionComponent(
        asset_name="drift_conversations",
        partition_type="daily",
        partition_start="2026-06-01",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def], resources={"drift_resource": resource}, partition_key="2026-06-02"
    )
    assert result.success
    mats = result.asset_materializations_for_node("drift_conversations")
    assert mats[-1].metadata["from_date_time"].value == "2026-06-02T00:00:00Z"
    assert mats[-1].metadata["to_date_time"].value == "2026-06-03T00:00:00Z"
    df = result.output_for_node("drift_conversations")
    assert len(df) == 1  # 06-02T12:00 is inside the partition's window


def test_bad_partition_config_raises_value_error(mod):
    component = mod.DriftConversationsIngestionComponent(
        asset_name="drift_conversations", partition_type="daily"
    )
    with pytest.raises(ValueError, match="requires partition_start"):
        component.build_defs(context=None)
