"""Per-resource description/owners/tags/metadata/asset_key (multi_asset mode)
and overridable _build_resources/_build_pipeline seams -- ported from
DltYamlSourceComponent."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakePipeline, install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        asset_shape="multi_asset",
        client={"base_url": "https://api.example.com"},
        owners=["team:default"],
        asset_tags={"domain": "shared"},
        resources=[{"name": "customers", "endpoint": {"path": "customers"}}],
    )
    kwargs.update(overrides)
    return mod.DltRestApiSourceComponent(**kwargs)


def test_per_resource_description_owners_tags_metadata(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1]})})
    component = _component(mod, resources=[{
        "name": "customers", "endpoint": {"path": "customers"},
        "description": "Customer records", "owners": ["team:crm"], "tags": {"pii": "true"},
        "metadata": {"source_system": "crm"},
    }])
    asset_def = component.build_defs(context=None).assets[0]
    spec = next(s for s in asset_def.specs if s.key == dg.AssetKey("test_asset_customers"))
    assert spec.description == "Customer records"
    assert list(spec.owners) == ["team:crm"]
    assert spec.tags.get("pii") == "true"
    assert spec.tags.get("domain") == "shared"
    assert spec.metadata.get("source_system") == "crm"


def test_per_resource_asset_key_override(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"customers": pd.DataFrame({"id": [1, 2]})})
    component = _component(mod, resources=[{"name": "customers", "endpoint": {"path": "customers"}, "asset_key": ["crm", "customers"]}])
    asset_def = component.build_defs(context=None).assets[0]
    assert dg.AssetKey(["crm", "customers"]) in set(asset_def.keys)
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result


def test_invalid_asset_key_type_raises(mod):
    component = _component(mod, resources=[{"name": "customers", "endpoint": {"path": "customers"}, "asset_key": 123}])
    with pytest.raises(ValueError, match="asset_key must be"):
        component.build_defs(context=None)


def test_build_resources_and_build_pipeline_are_real_overridable_methods(mod):
    component = _component(mod, asset_shape="single")
    assert callable(getattr(component, "_build_resources", None))
    assert callable(getattr(component, "_build_pipeline", None))


def test_subclass_can_override_build_pipeline(mod):
    calls = []

    class DemoModeComponent(mod.DltRestApiSourceComponent):
        def _build_pipeline(self, pipeline_name, dataset_name):
            calls.append((pipeline_name, dataset_name))
            return FakePipeline(table_rows={"orders": pd.DataFrame({"id": [1]})})

    component = DemoModeComponent(
        asset_name="a", client={"base_url": "https://x"},
        destination="snowflake", destination_credentials_env_var="UNSET",
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    assert calls == [("a_pipeline", "a")]
