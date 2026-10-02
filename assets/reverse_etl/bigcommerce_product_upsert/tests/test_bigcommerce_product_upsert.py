"""Committed regression tests for BigCommerceProductUpsertComponent.

The real BigCommerce Catalog API is never hit -- FakeBigCommerceResource
(conftest.py) stands in for the one external HTTP boundary, while
everything this component actually owns -- dual source resolution,
validation, row-building, create/update branching, error aggregation,
and metadata -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeBigCommerceResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_products", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"bigcommerce": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation -----------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.BigCommerceProductUpsertComponent(
            asset_name="x",
            fields_map={"sku_col": "sku"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.BigCommerceProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"sku_col": "sku"},
        ).build_defs(context=None)


def test_fields_map_missing_sku_raises(mod):
    with pytest.raises(ValueError, match="must include a"):
        mod.BigCommerceProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"name_col": "name"},
        ).build_defs(context=None)


# --- empty / batch-capping / skip behavior --------------------------------

def test_empty_upstream_df_no_upsert_calls(mod):
    df = pd.DataFrame({"sku_col": [], "name_col": []})
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({
        "sku_col": [f"SKU-{i}" for i in range(10)],
        "name_col": [f"Product {i}" for i in range(10)],
    })
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 3
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_upserted"] == 3


def test_rows_with_blank_or_null_sku_are_skipped_and_counted(mod):
    df = pd.DataFrame({
        "sku_col": ["SKU-1", None, ""],
        "name_col": ["Product 1", "Product 2", "Product 3"],
    })
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 1
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_upserted"] == 1
    assert out["rows_skipped_no_sku"] == 2


# --- create vs update branching -------------------------------------------

def test_new_sku_creates(mod):
    df = pd.DataFrame({
        "sku_col": ["NEW-SKU"],
        "name_col": ["Brand New Widget"],
        "type_col": ["physical"],
        "price_col": [19.99],
        "weight_col": [1.5],
    })
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={
            "sku_col": "sku",
            "name_col": "name",
            "type_col": "type",
            "price_col": "price",
            "weight_col": "weight",
        },
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.products["NEW-SKU"]["name"] == "Brand New Widget"
    assert resource.products["NEW-SKU"]["type"] == "physical"
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["match_key"] == "sku"
    assert out["bigcommerce_object_type"] == "Product"


def test_existing_sku_updates(mod):
    resource = FakeBigCommerceResource(existing={"EXISTING-SKU": {"name": "Old Name", "price": 10.0}})
    df = pd.DataFrame({
        "sku_col": ["EXISTING-SKU"],
        "name_col": ["New Name"],
    })
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.products["EXISTING-SKU"]["name"] == "New Name"
    # Untouched field preserved -- partial update, not a full overwrite.
    assert resource.products["EXISTING-SKU"]["price"] == 10.0
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_duplicate_sku_in_same_batch_creates_once_then_updates(mod):
    df = pd.DataFrame({
        "sku_col": ["DUP-SKU", "DUP-SKU"],
        "name_col": ["First Pass", "Second Pass"],
    })
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    # Only one upsert_product_by_sku call (the first row); the second row
    # hits the in-run cache and calls update_product directly.
    assert len(resource.upsert_calls) == 1
    assert len(resource.update_calls) == 1
    assert resource.products["DUP-SKU"]["name"] == "Second Pass"
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1


# --- null handling ----------------------------------------------------------

def test_null_value_in_optional_column_omitted_from_body(mod):
    df = pd.DataFrame({
        "sku_col": ["SKU-X"],
        "name_col": ["Widget"],
        "desc_col": [None],
    })
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name", "desc_col": "description"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    _, body = resource.upsert_calls[0]
    assert "description" not in body
    assert body["name"] == "Widget"


# --- error aggregation ------------------------------------------------------

def test_error_from_resource_is_aggregated_not_raised(mod):
    resource = FakeBigCommerceResource()
    resource.raise_for_sku["BAD-SKU"] = RuntimeError("simulated 422 from BigCommerce")
    df = pd.DataFrame({
        "sku_col": ["GOOD-SKU", "BAD-SKU"],
        "name_col": ["Good Product", "Bad Product"],
    })
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_created"] == 1
    assert out["rows_errored"] == 1
    assert "BAD-SKU" in out["first_errors"][0]
    assert "GOOD-SKU" in resource.products


# --- source: inline mode -----------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeBigCommerceResource()
    component = mod.BigCommerceProductUpsertComponent(
        asset_name="bigcommerce_out",
        source={"kind": "inline", "rows": [
            {"sku_col": "A1", "name_col": "Alpha"},
            {"sku_col": "B2", "name_col": "Beta"},
        ]},
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"bigcommerce": resource})
    assert result.success
    out = _metadata_for(result, "bigcommerce_out")
    assert out["rows_upserted"] == 2
    assert "A1" in resource.products
    assert "B2" in resource.products
