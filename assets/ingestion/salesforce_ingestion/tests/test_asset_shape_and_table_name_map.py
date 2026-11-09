"""Tests for SalesforceIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion/zendesk_ingestion, adapted for Salesforce's lowercased
resource-attribute convention (e.g. sf_objects=["Account"] -> source.account, destination
table "account" by default).

`asset_shape='single'` (default, unchanged behavior) keeps every object unioned into one
asset -- see test_salesforce_ingestion.py for that coverage. `asset_shape='multi_asset'`
gives each object its own independently-materializable asset via
`@multi_asset(can_subset=True)`, subsetting dlt's own source object with
`source.with_resources(*names)` (the real dlt mechanism verified live before this rollout
began) rather than servicenow's hand-built resources list.

`table_name_map` renames an object's destination table via dlt's own
`DltResource.apply_hints(table_name=...)` -- keys are matched case-insensitively against
`sf_objects` since the field accepts Salesforce's natural CamelCase object names.
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
        username="u",
        password="p",
        security_token="t",
        sf_objects=["Account", "Contact"],
    )
    kwargs.update(overrides)
    return mod.SalesforceIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_object(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "account": pd.DataFrame({"id": [1, 2]}),
        "contact": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_account"), dg.AssetKey("test_asset_contact")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    account_df = result.output_for_node("test_asset", output_name="test_asset_account")
    contact_df = result.output_for_node("test_asset", output_name="test_asset_contact")
    assert list(account_df["id"]) == [1, 2]
    assert list(contact_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_object_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "account": pd.DataFrame({"id": [1, 2]}),
        "contact": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_account")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("account" in q for q in queries)
    assert not any("contact" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_accounts": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, sf_objects=["Account"], table_name_map={"Account": "raw_accounts"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_accounts": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, sf_objects=["Account"], asset_shape="multi_asset", table_name_map={"Account": "raw_accounts"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_account")
    assert len(df) == 3


def test_table_name_map_is_case_insensitive_against_sf_objects(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_accounts": pd.DataFrame({"id": [1]})})
    component = _make_component(mod, sf_objects=["Account"], table_name_map={"account": "raw_accounts"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert len(result.output_for_node("test_asset")) == 1


def test_table_name_map_with_unknown_object_raises_a_clear_error(mod):
    component = _make_component(mod, sf_objects=["Account"], table_name_map={"Contact": "raw_contacts"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
