"""Tests for FacebookAdsIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion/zendesk_ingestion. This component is more complex
than its siblings: "insights" is loaded via a SEPARATE dlt source
(`facebook_insights_source`) from every other resource (`facebook_ads_source`), so a
run mixing a standard resource with "insights" makes two separate `pipeline.run()`
calls. The shared `_run_pipeline` helper (used by both asset shapes) handles this
split; these tests confirm selecting a subset still correctly skips the source whose
resources weren't selected.
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
        account_id="act_1",
        access_token="tok",
        app_id="app",
        app_secret="secret",
        resources="campaigns,insights",
    )
    kwargs.update(overrides)
    return mod.FacebookAdsIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "campaigns": pd.DataFrame({"id": [1, 2]}),
        "insights": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_campaigns"), dg.AssetKey("test_asset_insights")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    campaigns_df = result.output_for_node("test_asset", output_name="test_asset_campaigns")
    insights_df = result.output_for_node("test_asset", output_name="test_asset_insights")
    assert list(campaigns_df["id"]) == [1, 2]
    assert list(insights_df["id"]) == [9]


def test_multi_asset_shape_selecting_standard_resource_never_runs_insights_source(mod, monkeypatch):
    """The core requirement, adapted for this component's two-dlt-source split: selecting just
    'campaigns' (a facebook_ads_source resource) must not even call facebook_insights_source."""
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "campaigns": pd.DataFrame({"id": [1, 2]}),
        "insights": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_campaigns")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("campaigns" in q for q in queries)
    assert not any("insights" in q for q in queries)
    # Only one pipeline.run() call -- facebook_insights_source's branch never executed.
    assert len(fake_pipeline.run_calls) == 1


def test_multi_asset_shape_selecting_only_insights_never_runs_standard_source(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "campaigns": pd.DataFrame({"id": [1, 2]}),
        "insights": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_insights")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("insights" in q for q in queries)
    assert not any("campaigns" in q for q in queries)
    assert len(fake_pipeline.run_calls) == 1


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_insights": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, resources="insights", table_name_map={"insights": "raw_insights"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_insights": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, resources="insights", asset_shape="multi_asset", table_name_map={"insights": "raw_insights"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_insights")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, resources="campaigns", table_name_map={"insights": "raw_insights"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
