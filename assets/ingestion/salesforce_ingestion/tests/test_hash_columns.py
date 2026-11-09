"""hash_columns: salted SHA-256 pseudonymization wired onto every resource via
DltResource.add_map() before pipeline.run(), for both asset_shape branches
(single -> selected_resources list, multi_asset -> subset_source.resources.values()).

dlt.sources.salesforce is a verified source not installed in this dev environment (see
conftest.py), faked wholesale via `_Permissive` -- which has no real pipe/steps
machinery to introspect, unlike dlt_load_collection/activecampaign_ingestion's tests
(both exercise a REAL, installed dlt.sources.rest_api.rest_api_source directly). So
these tests verify the wiring runs without error and that validation still raises
clearly, rather than asserting an exact pipe-step count."""
import dagster as dg
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
        sf_objects=["Account"],
    )
    kwargs.update(overrides)
    return mod.SalesforceIngestionComponent(**kwargs)


def test_hash_columns_single_shape_does_not_crash(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, hash_columns=["Email"], hash_salt="s")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_hash_columns_multi_asset_does_not_crash(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, hash_columns=["Email"], hash_salt="s", asset_shape="multi_asset")
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_hash_salt_required_when_hash_columns_set(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod, hash_columns=["Email"])
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="hash_salt"):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_hash_salt_env_var_unset_raises_clearly(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    monkeypatch.delenv("SALESFORCE_HASH_SALT_TEST_UNSET", raising=False)
    component = _make_component(
        mod, hash_columns=["Email"], hash_salt_env_var="SALESFORCE_HASH_SALT_TEST_UNSET",
    )
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="SALESFORCE_HASH_SALT_TEST_UNSET"):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_no_hash_columns_is_a_no_op(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
