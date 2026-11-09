"""Tests for JiraIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion/zendesk_ingestion/salesforce_ingestion/hubspot_ingestion.

`asset_shape='single'` (default, unchanged behavior) keeps every resource unioned into
one asset -- see test_jira_ingestion.py for that coverage. `asset_shape='multi_asset'`
gives each resource its own independently-materializable asset via
`@multi_asset(can_subset=True)`, subsetting dlt's own source object with
`source.with_resources(*names)`.

`table_name_map` renames a resource's destination table via dlt's own
`DltResource.apply_hints(table_name=...)`.
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
        domain="acme",
        email="me@acme.com",
        api_token="tok",
        resources=["issues", "projects"],
    )
    kwargs.update(overrides)
    return mod.JiraIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "issues": pd.DataFrame({"id": [1, 2]}),
        "projects": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_issues"), dg.AssetKey("test_asset_projects")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    issues_df = result.output_for_node("test_asset", output_name="test_asset_issues")
    projects_df = result.output_for_node("test_asset", output_name="test_asset_projects")
    assert list(issues_df["id"]) == [1, 2]
    assert list(projects_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "issues": pd.DataFrame({"id": [1, 2]}),
        "projects": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_issues")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("issues" in q for q in queries)
    assert not any("projects" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_issues": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, resources=["issues"], table_name_map={"issues": "raw_issues"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_issues": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, resources=["issues"], asset_shape="multi_asset", table_name_map={"issues": "raw_issues"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_issues")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, resources=["issues"], table_name_map={"projects": "raw_projects"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
