"""on_schema_change/exclude_columns/max_rows_per_resource for TealiumIngestionComponent.

This component uses a REAL `dlt.sources.rest_api.rest_api_source` call (same family as
activecampaign_ingestion), but conftest.py's own `install_fake_dlt` fakes it to return a
bare `object()` -- fine for config-capture checks, but it has no `.resources` to
exercise exclude_columns/hash_columns/max_rows_per_resource against. This file defines
its own fake returning a `_Permissive`-style stand-in instead, reusing conftest.py's
`install_fake_auth` (the inline Tealium v3 login POST) and `FakePipeline`/
`FakeSqlClient`/`FakeCursor` (the query-back machinery) unchanged.
"""
import pytest

from .conftest import FakePipeline, install_fake_auth, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _Permissive:
    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter([self])


def _install_fake_dlt_permissive(monkeypatch, mod):
    captured = {}
    fake_pipeline = FakePipeline(table_rows=None)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    def fake_rest_api_source(config):
        captured["config"] = config
        return _Permissive()

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    monkeypatch.setattr(
        mod.dlt.sources.rest_api, "rest_api_source", fake_rest_api_source, raising=False
    )
    return fake_pipeline, captured


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="tealium_out",
        account="mycompany",
        profile="main",
        username="user@example.com",
        api_key="key_abc123",
        lookup_attribute_id="crm_id",
        lookup_values="cust_1",
    )
    kwargs.update(overrides)
    return mod.TealiumIngestionComponent(**kwargs)


def _materialize(mod, component, monkeypatch):
    import dagster as dg

    install_fake_auth(monkeypatch)
    fake_pipeline, captured = _install_fake_dlt_permissive(monkeypatch, mod)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=True)
    return result, captured


def test_on_schema_change_does_not_crash(mod, monkeypatch):
    component = _make_component(mod, on_schema_change="block_all")
    result, captured = _materialize(mod, component, monkeypatch)
    assert result.success
    assert captured["config"]["resource_defaults"]["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_on_schema_change_default_does_not_set_resource_defaults(mod, monkeypatch):
    component = _make_component(mod)
    result, captured = _materialize(mod, component, monkeypatch)
    assert "resource_defaults" not in captured["config"]


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        _make_component(mod, on_schema_change="bogus")


def test_exclude_columns_does_not_crash(mod, monkeypatch):
    component = _make_component(mod, exclude_columns=["notes"])
    result, _ = _materialize(mod, component, monkeypatch)
    assert result.success


def test_exclude_columns_applied_before_hash_columns(mod):
    exclude_map = mod._make_exclude_columns_map(["email"])
    hash_map = mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}


def test_max_rows_per_resource_does_not_crash(mod, monkeypatch):
    component = _make_component(mod, max_rows_per_resource=10)
    result, _ = _materialize(mod, component, monkeypatch)
    assert result.success


def test_no_advanced_options_set_is_a_no_op(mod, monkeypatch):
    component = _make_component(mod)
    result, _ = _materialize(mod, component, monkeypatch)
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
