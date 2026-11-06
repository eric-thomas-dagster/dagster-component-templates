"""Per-resource description/owners/tags/metadata/asset_key overrides.
Real gap reported from production usage: each source connection was one
component instance with shared owners/tags, and no way to key a resource's
asset under anything but the forced '{asset_name}_{resource}' single
segment. Only meaningful in asset_shape='multi_asset' (single-shape has one
asset, so there's no per-resource asset identity to set them on)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        source_type="rest_api",
        asset_shape="multi_asset",
        client={"base_url": "https://api.example.com"},
        owners=["team:default"],
        asset_tags={"domain": "shared"},
    )
    kwargs.update(overrides)
    return mod.DltYamlSourceComponent(**kwargs)


def test_per_resource_description_owners_tags_metadata(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1]}), "orders": pd.DataFrame({"id": [1]})})
    component = _component(
        mod,
        rest_api_resources=[
            {
                "name": "customers",
                "endpoint": {"path": "customers"},
                "description": "Customer records",
                "owners": ["team:crm"],
                "tags": {"pii": "true"},
                "metadata": {"source_system": "crm"},
            },
            {"name": "orders", "endpoint": {"path": "orders"}},
        ],
    )
    asset_def = component.build_defs(context=None).assets[0]
    specs = {s.key: s for s in asset_def.specs}

    customers_spec = specs[dg.AssetKey("test_asset_customers")]
    assert customers_spec.description == "Customer records"
    assert list(customers_spec.owners) == ["team:crm"]
    assert customers_spec.tags.get("pii") == "true"
    assert customers_spec.tags.get("domain") == "shared"  # component-level tag still merged in
    assert customers_spec.metadata.get("source_system") == "crm"

    orders_spec = specs[dg.AssetKey("test_asset_orders")]
    assert list(orders_spec.owners) == ["team:default"]  # falls back to component-level owners
    assert orders_spec.tags.get("domain") == "shared"


def test_per_resource_asset_key_override(mod, monkeypatch):
    # asset_key only overrides the Dagster-facing asset key, not the destination table name.
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1, 2]})})
    component = _component(
        mod,
        rest_api_resources=[{"name": "customers", "endpoint": {"path": "customers"}, "asset_key": ["crm", "customers"]}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert dg.AssetKey(["crm", "customers"]) in set(asset_def.keys)

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    df = result.output_for_node("test_asset", output_name="test_asset_customers")
    assert len(df) == 2


def test_per_resource_asset_key_as_plain_string(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1]})})
    component = _component(
        mod,
        rest_api_resources=[{"name": "customers", "endpoint": {"path": "customers"}, "asset_key": "crm_customers"}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert dg.AssetKey("crm_customers") in set(asset_def.keys)


def test_invalid_asset_key_type_raises_a_clear_error(mod):
    component = _component(mod, rest_api_resources=[{"name": "customers", "endpoint": {"path": "customers"}, "asset_key": 123}])
    with pytest.raises(ValueError, match="asset_key must be"):
        component.build_defs(context=None)
