"""on_schema_change/exclude_columns/max_rows_per_resource for PostmanIngestionComponent.

This component's conftest.py fakes `rest_api_source` itself (returning a bare
`object()`, captured via `captured["config"]`) rather than a real DltSource with
`.resources` -- so `on_schema_change` (injected into `config["resource_defaults"]`
BEFORE `rest_api_source(config)` is called) is fully verifiable here, but
exclude_columns/max_rows_per_resource (applied to `source.resources.values()` AFTER)
are not reachable through this fake. Those two are covered at the unit level instead
(same shared helpers every component in this rollout uses), and end-to-end against a
real rest_api_source in activecampaign_ingestion's/dlt_load_collection's own tests.
"""
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None):
    import dagster as dg

    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=True)
    return result, fake_pipeline, captured


def test_on_schema_change_default_does_not_set_resource_defaults(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(asset_name="postman_out", api_key="k")
    _result, _pipeline, captured = _materialize(
        mod, component, table_rows={"collections": pd.DataFrame({"id": [1]})}, monkeypatch=monkeypatch,
    )
    assert "resource_defaults" not in captured["config"]


def test_on_schema_change_block_all_sets_resource_defaults(mod, monkeypatch):
    component = mod.PostmanIngestionComponent(asset_name="postman_out", api_key="k", on_schema_change="block_all")
    _result, _pipeline, captured = _materialize(
        mod, component, table_rows={"collections": pd.DataFrame({"id": [1]})}, monkeypatch=monkeypatch,
    )
    assert captured["config"]["resource_defaults"] == {
        "schema_contract": {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    }


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        mod.PostmanIngestionComponent(asset_name="a", api_key="k", on_schema_change="bogus")


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}


def test_exclude_columns_applied_before_hash_columns(mod):
    exclude_map = mod._make_exclude_columns_map(["email"])
    hash_map = mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}
