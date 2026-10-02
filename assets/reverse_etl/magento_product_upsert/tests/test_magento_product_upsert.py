"""Committed regression tests for MagentoProductUpsertComponent.

The real `requests`-backed MagentoResource is never imported here -- a
minimal FakeMagentoResource (conftest.py) stands in for the one external
HTTP boundary, while everything this component actually owns -- dual
source resolution, validation, row-building, batch capping, error
aggregation, and metadata -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeMagentoResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_products", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"magento": resource})


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
        mod.MagentoProductUpsertComponent(
            asset_name="x",
            fields_map={"sku_col": "sku"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MagentoProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            fields_map={"sku_col": "sku"},
        ).build_defs(context=None)


def test_sku_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="fields_map must include a mapping to Magento field `sku`"):
        mod.MagentoProductUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            fields_map={"name_col": "name"},
        ).build_defs(context=None)


# --- empty upstream ---------------------------------------------------------

def test_empty_upstream_upserts_nothing(mod):
    df = pd.DataFrame({"sku_col": [], "name_col": []})
    resource = FakeMagentoResource()
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_upserted"] == 0


# --- batch_size capping ------------------------------------------------------

def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({
        "sku_col": [f"SKU-{i}" for i in range(10)],
        "name_col": [f"Product {i}" for i in range(10)],
    })
    resource = FakeMagentoResource()
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 3
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_upserted"] == 3


# --- rows skipped when sku blank --------------------------------------------

def test_rows_with_blank_or_null_sku_are_skipped_and_counted(mod):
    df = pd.DataFrame({
        "sku_col": ["SKU-1", None, "", "SKU-4"],
        "name_col": ["Widget", "Gadget", "Gizmo", "Thing"],
    })
    resource = FakeMagentoResource()
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls) == 2
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_upserted"] == 2
    assert out["rows_skipped_no_sku"] == 2


# --- create vs update branching, proving update-only semantics -------------

def test_create_vs_update_branching_matches_real_update_only_semantics(mod):
    """Proves PUT (update_calls) is only ever exercised for a sku already
    present in the fake's "existing products" -- the same update-only
    semantics as the real Magento REST API (PUT never creates)."""
    df = pd.DataFrame({
        "sku_col": ["EXISTING-1", "NEW-1"],
        "name_col": ["Existing Widget", "Brand New Widget"],
        "price_col": [19.99, 29.99],
    })
    resource = FakeMagentoResource(
        existing_products={"EXISTING-1": {"sku": "EXISTING-1", "name": "Old Name", "price": 9.99}}
    )
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name", "price_col": "price"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    # update_calls only ever happened for the sku that pre-existed.
    assert [c["sku"] for c in resource.update_calls] == ["EXISTING-1"]
    # create_calls only ever happened for the sku that did NOT pre-exist.
    assert [c["sku"] for c in resource.create_calls] == ["NEW-1"]

    out = _metadata_for(result, "magento_products_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2
    assert out["match_key"] == "sku"
    assert out["magento_object_type"] == "Product"


def test_update_product_rejects_nonexistent_sku_in_fake_mirroring_real_api(mod):
    """Sanity check on the fake itself: calling update_product directly for
    a sku that was never created raises, same as the real Magento PUT
    endpoint would effectively refuse to create via PUT."""
    resource = FakeMagentoResource()
    with pytest.raises(RuntimeError, match="update-only"):
        resource.update_product("GHOST-SKU", {"name": "Ghost"})


# --- error aggregation -------------------------------------------------------

def test_errors_from_resource_are_aggregated_not_fatal(mod):
    df = pd.DataFrame({
        "sku_col": ["GOOD-1", "BAD-1", "GOOD-2"],
        "name_col": ["Good One", "Bad One", "Good Two"],
    })
    resource = FakeMagentoResource(raise_on_sku={"BAD-1"})
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success  # errors are aggregated, not raised -- asset still succeeds
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_errored"] == 1
    assert out["rows_upserted"] == 2
    assert "BAD-1" in out["first_errors"][0]


# --- source: {kind: inline} mode ---------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeMagentoResource()
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        source={
            "kind": "inline",
            "rows": [
                {"sku_col": "INLINE-1", "name_col": "Inline Widget"},
                {"sku_col": "INLINE-2", "name_col": "Inline Gadget"},
            ],
        },
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"magento": resource})
    assert result.success
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_upserted"] == 2
    assert [sku for sku, _ in resource.upsert_calls] == ["INLINE-1", "INLINE-2"]


# --- metadata field values ----------------------------------------------------

def test_metadata_field_values_are_exact(mod):
    df = pd.DataFrame({
        "sku_col": ["SKU-A", "SKU-B", "SKU-C"],
        "name_col": ["A", "B", "C"],
    })
    resource = FakeMagentoResource(existing_products={"SKU-B": {"sku": "SKU-B", "name": "Old B"}})
    component = mod.MagentoProductUpsertComponent(
        asset_name="magento_products_out",
        upstream_asset_key="upstream_products",
        fields_map={"sku_col": "sku", "name_col": "name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "magento_products_out")
    assert out["rows_created"] == 2
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 3
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_sku"] == 0
    assert out["match_key"] == "sku"
    assert out["magento_object_type"] == "Product"
