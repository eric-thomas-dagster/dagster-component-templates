"""Tests for StripeIngestionComponent's `asset_shape` and `table_name_map` fields --
same rollout as servicenow_ingestion/zendesk_ingestion, adapted for Stripe's `resources`
comma-separated STRING field (not a List[str]) and its two source factories
(stripe_source / incremental_stripe_source).

Unlike zendesk (which subsets an already-built source via `with_resources(*names)`),
the multi_asset branch here rebuilds the Stripe source with `endpoints=tuple(selected)`
directly -- cleaner given stripe_source's own `endpoints=` kwarg already restricts which
resources exist on the source, so unselected resources are never even requested from
Stripe's API.

`table_name_map` renames a resource's destination table via dlt's own
`DltResource.apply_hints(table_name=...)` (a real, live-verified dlt API) -- distinct
from the resource's `name`, which stays the asset-key-driving identifier.
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
        api_key="sk_test_123",
        resources="customers,charges",
    )
    kwargs.update(overrides)
    return mod.StripeIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "charges": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_customers"), dg.AssetKey("test_asset_charges")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    customers_df = result.output_for_node("test_asset", output_name="test_asset_customers")
    charges_df = result.output_for_node("test_asset", output_name="test_asset_charges")
    assert list(customers_df["id"]) == [1, 2]
    assert list(charges_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "customers": pd.DataFrame({"id": [1, 2]}),
        "charges": pd.DataFrame({"id": [9]}),
    })
    component = _make_component(mod, asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_customers")],
    )
    assert result.success

    queries = fake_pipeline.sql_client_instance.queries
    assert any("customers" in q for q in queries)
    assert not any("charges" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(mod, resources="customers", table_name_map={"customers": "raw_customers"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_customers": pd.DataFrame({"id": [1, 2, 3]})})
    component = _make_component(
        mod, resources="customers", asset_shape="multi_asset", table_name_map={"customers": "raw_customers"}
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_customers")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, resources="customers", table_name_map={"charges": "raw_charges"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)
