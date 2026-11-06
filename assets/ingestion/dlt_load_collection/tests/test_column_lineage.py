"""column_lineage, ported from dlt_rest_api_source. Scoped to asset_shape='single'
only -- 'multi_asset' has several output assets with no single obvious
upstream-to-output column mapping."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_column_lineage_emitted_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"revenue": [10]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
        deps=["raw_sales"],
        column_lineage={"revenue": ["price", "quantity"]},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    event = next(
        e for e in result.get_asset_materialization_events()
        if "dagster/column_lineage" in e.materialization.metadata
    )
    lineage = event.materialization.metadata["dagster/column_lineage"].value
    assert lineage.deps_by_column["revenue"][0].asset_key == dg.AssetKey("raw_sales")


def test_column_lineage_absent_by_default(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    event = next(iter(result.get_asset_materialization_events()))
    assert "dagster/column_lineage" not in event.materialization.metadata
