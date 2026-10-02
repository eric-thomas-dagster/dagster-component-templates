"""Committed regression tests for WooCommerceProductUpsertComponent.

The real WooCommerceResource (and the HTTP it wraps) is never imported --
FakeWooCommerceResource (conftest.py) stands in for the one external,
paid-API boundary, while everything this component actually owns --
validation, dual source resolution, row-building/fields_map application,
create-vs-update branching, error aggregation, and metadata -- is
exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeWooCommerceResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_products", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"woocommerce": resource})


def _materialize_source_only(component, resource):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={"woocommerce": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation ------------------------------------------------------------


def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WooCommerceProductUpsertComponent(
            asset_name="x",
            fields_map={"product_sku": "sku"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WooCommerceProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"product_sku": "sku"},
        ).build_defs(context=None)


def test_sku_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="must include a"):
        mod.WooCommerceProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"product_name": "name"},
        ).build_defs(context=None)


# --- empty upstream ----------------------------------------------------------


def test_empty_upstream_dataframe_is_a_noop(mod):
    df = pd.DataFrame(columns=["product_sku", "product_name"])
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_upserted"] == 0


# --- batch_size capping ------------------------------------------------------


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame(
        {
            "product_sku": [f"sku-{i}" for i in range(10)],
            "product_name": [f"Product {i}" for i in range(10)],
        }
    )
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
        batch_size=4,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 4
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_upserted"] == 4


# --- rows skipped when sku blank --------------------------------------------


def test_rows_with_blank_or_null_sku_are_skipped(mod):
    df = pd.DataFrame(
        {
            "product_sku": ["sku-1", None, "", "sku-4"],
            "product_name": ["A", "B", "C", "D"],
        }
    )
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 2
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_skipped_no_sku"] == 2
    assert out["rows_upserted"] == 2


# --- create vs update branching ---------------------------------------------


def test_new_sku_creates_then_same_sku_updates(mod):
    df = pd.DataFrame(
        {
            "product_sku": ["sku-1", "sku-1"],
            "product_name": ["First write", "Second write"],
        }
    )
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "woocommerce_products_out")
    # First row creates (new sku); second row -- same sku, cached id from
    # this run -- hits update_product directly rather than a second upsert.
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert len(resource.upsert_calls) == 1
    assert len(resource.update_calls) == 1


def test_preexisting_sku_updates_via_resource_search(mod):
    resource = FakeWooCommerceResource()
    # Simulate a product that already exists in WooCommerce before this run.
    resource._by_sku["sku-existing"] = {"id": 5000, "sku": "sku-existing", "name": "Old Name"}
    df = pd.DataFrame({"product_sku": ["sku-existing"], "product_name": ["New Name"]})
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1
    assert resource._by_sku["sku-existing"]["name"] == "New Name"


# --- error aggregation --------------------------------------------------


def test_errors_from_resource_are_aggregated_not_raised(mod):
    df = pd.DataFrame(
        {
            "product_sku": ["sku-ok", "sku-bad"],
            "product_name": ["Good", "Bad"],
        }
    )
    resource = FakeWooCommerceResource()
    resource.raise_for_sku["sku-bad"] = RuntimeError("simulated 500 from WooCommerce")
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_created"] == 1
    assert out["rows_errored"] == 1
    assert "sku-bad" in out["first_errors"][0]


# --- source: {kind: inline} mode ---------------------------------------------


def test_source_inline_mode(mod):
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        source={
            "kind": "inline",
            "rows": [
                {"product_sku": "sku-a", "product_name": "Alpha"},
                {"product_sku": "sku-b", "product_name": "Beta"},
            ],
        },
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize_source_only(component, resource)
    assert result.success
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["rows_upserted"] == 2
    assert len(resource.upsert_calls) == 2


# --- metadata field values ----------------------------------------------


def test_metadata_match_key_and_object_type(mod):
    df = pd.DataFrame({"product_sku": ["sku-1"], "product_name": ["A"]})
    resource = FakeWooCommerceResource()
    component = mod.WooCommerceProductUpsertComponent(
        asset_name="woocommerce_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"product_sku": "sku", "product_name": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "woocommerce_products_out")
    assert out["woocommerce_object_type"] == "Product"
    assert out["match_key"] == "sku"
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_sku"] == 0
