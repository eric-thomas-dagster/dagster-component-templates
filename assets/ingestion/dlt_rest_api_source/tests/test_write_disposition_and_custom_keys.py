"""write_disposition inference/validation, merge_key, and the 'x_' custom-key
convention -- ported from DltLoadCollectionComponent (same resources:
List[Dict] shape, same risk profile)."""
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
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    kwargs.update(overrides)
    return mod.DltRestApiSourceComponent(**kwargs)


def test_primary_key_with_no_explicit_write_disposition_infers_merge(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = _component(mod, resources=[{"name": "orders", "primary_key": "id", "endpoint": {"path": "orders"}}])
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert captured["config"]["resources"][0]["write_disposition"] == "merge"


def test_explicit_write_disposition_wins(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = _component(mod, resources=[{"name": "orders", "primary_key": "id", "write_disposition": "append", "endpoint": {"path": "orders"}}])
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert captured["config"]["resources"][0]["write_disposition"] == "append"


def test_no_primary_key_defaults_to_dlts_own_append(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = _component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert "write_disposition" not in captured["config"]["resources"][0]


def test_invalid_write_disposition_raises(mod):
    component = _component(mod, resources=[{"name": "orders", "write_disposition": "upsert", "endpoint": {"path": "orders"}}])
    with pytest.raises(ValueError, match="write_disposition must be"):
        component.build_defs(context=None)


def test_merge_without_key_raises_a_clear_error(mod):
    component = _component(mod, resources=[{"name": "orders", "write_disposition": "merge", "endpoint": {"path": "orders"}}])
    with pytest.raises(ValueError, match="no natural key to dedupe on"):
        component.build_defs(context=None)


def test_merge_with_merge_key_only_is_accepted(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = _component(mod, resources=[{"name": "orders", "write_disposition": "merge", "merge_key": "id", "endpoint": {"path": "orders"}}])
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert captured["config"]["resources"][0]["write_disposition"] == "merge"
    assert captured["config"]["resources"][0]["merge_key"] == "id"


def test_x_prefixed_keys_are_stripped_before_reaching_rest_api_source(mod, monkeypatch):
    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = _component(mod, resources=[{
        "name": "orders", "endpoint": {"path": "orders"}, "x_min_rows": 10, "x_contract_columns": ["id"],
    }])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    assert "x_min_rows" not in captured["config"]["resources"][0]
    assert "x_contract_columns" not in captured["config"]["resources"][0]


def test_subclass_can_read_x_prefixed_keys_from_self_resources(mod):
    component = _component(mod, resources=[{"name": "orders", "endpoint": {"path": "orders"}, "x_min_rows": 10}])
    assert component.resources[0]["x_min_rows"] == 10
