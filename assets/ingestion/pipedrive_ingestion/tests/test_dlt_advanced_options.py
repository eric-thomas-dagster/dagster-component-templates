"""on_schema_change/exclude_columns/max_rows_per_resource for the getattr-style
(verified-source) family.

dlt.sources.pipedrive is a verified source not installed in this dev environment (see
conftest.py), faked wholesale via `_Permissive` -- which has no real pipe/steps
machinery to introspect, unlike dlt_load_collection/activecampaign_ingestion's tests
(both exercise a REAL, installed dlt source directly). So these tests verify the
wiring runs without error, rather than asserting an exact pipe-step count -- same
convention as test_hash_columns.py / salesforce_ingestion's test_dlt_advanced_options.py.
"""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="test_asset", api_token="tok")
    kwargs.update(overrides)
    return mod.PipedriveIngestionComponent(**kwargs)


def test_on_schema_change_does_not_crash(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, on_schema_change="block_all")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        _make_component(mod, on_schema_change="bogus")


def test_exclude_columns_does_not_crash(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, exclude_columns=["Notes"])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_exclude_columns_applied_before_hash_columns(mod):
    """A column in both ends up excluded, not hashed-then-excluded -- unit-level check,
    same shared helpers as every other component in this rollout."""
    exclude_map = mod._make_exclude_columns_map(["Email"])
    hash_map = mod._make_hash_columns_map(["Email"], "s")
    assert hash_map(exclude_map({"Email": "a@b.com"})) == {}


def test_max_rows_per_resource_does_not_crash(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, max_rows_per_resource=10)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_no_advanced_options_set_is_a_no_op(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}
