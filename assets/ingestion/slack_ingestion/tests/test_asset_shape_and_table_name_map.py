"""Tests for SlackIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as zendesk_ingestion/servicenow_ingestion, for a dlt VERIFIED source
(slack_source) with attribute-based resource access.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        api_token="tok",
        resources=["messages", "channels"],
    )
    kwargs.update(overrides)
    return mod.SlackIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "messages": pd.DataFrame({"id": [1, 2]}),
        "channels": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_messages"), dg.AssetKey("test_asset_channels")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    messages_df = result.output_for_node("test_asset", output_name="test_asset_messages")
    channels_df = result.output_for_node("test_asset", output_name="test_asset_channels")
    assert list(messages_df["id"]) == [1, 2]
    assert list(channels_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "messages": pd.DataFrame({"id": [1, 2]}),
        "channels": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_messages")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("messages" in q for q in queries)
    assert not any("channels" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_messages": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, resources=["messages"], table_name_map={"messages": "raw_messages"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_messages": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, resources=["messages"], asset_shape="multi_asset", table_name_map={"messages": "raw_messages"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_messages")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, resources=["messages"], table_name_map={"channels": "raw_channels"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
