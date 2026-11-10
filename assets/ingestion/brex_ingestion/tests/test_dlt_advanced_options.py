"""on_schema_change/exclude_columns/max_rows_per_resource: real HTTP call never made
here (rest_api_source is only built, then materialized against a faked
dlt.pipeline().run() that never iterates the resource generators), so these check the
resource's pipe/config gained the expected step or config change rather than fetching
real data -- same convention as activecampaign_ingestion's test_dlt_advanced_options.py.
"""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, monkeypatch, **overrides):
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    kwargs = dict(asset_name="test_asset", api_key='key')
    kwargs.update(overrides)
    component = mod.BrexIngestionComponent(**kwargs)
    defs = component.build_defs(context=None)
    dg.materialize(list(defs.assets), resources=defs.resources)
    return fake_pipeline.run_calls[-1]


def _step_counts(source):
    return {name: len(resource._pipe._steps) for name, resource in source.resources.items()}


# --- on_schema_change -----------------------------------------------------------


def test_on_schema_change_default_does_not_touch_resource_config(mod, monkeypatch):
    source = _materialize(mod, monkeypatch)
    for resource in source.resources.values():
        assert not resource.compute_table_schema().get("schema_contract")


def test_on_schema_change_block_all_sets_schema_contract(mod, monkeypatch):
    source = _materialize(mod, monkeypatch, on_schema_change="block_all")
    for resource in source.resources.values():
        contract = resource.compute_table_schema().get("schema_contract")
        assert contract == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        mod.BrexIngestionComponent(
            asset_name="a", api_key='key', on_schema_change="bogus",
        )


# --- exclude_columns -------------------------------------------------------------


def test_exclude_columns_adds_a_pipe_step_to_every_resource(mod, monkeypatch):
    plain = _step_counts(_materialize(mod, monkeypatch))
    excluded = _step_counts(_materialize(mod, monkeypatch, exclude_columns=["internal_notes"]))
    assert set(plain) == set(excluded) and plain
    for name in plain:
        assert excluded[name] == plain[name] + 1


def test_exclude_columns_applied_before_hash_columns(mod):
    exclude_map = mod._make_exclude_columns_map(["email"])
    hash_map = mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}


# --- max_rows_per_resource ---------------------------------------------------------


def test_max_rows_per_resource_adds_a_pipe_step_to_every_resource(mod, monkeypatch):
    plain = _step_counts(_materialize(mod, monkeypatch))
    capped = _step_counts(_materialize(mod, monkeypatch, max_rows_per_resource=10))
    assert set(plain) == set(capped) and plain
    for name in plain:
        assert capped[name] == plain[name] + 1


def test_max_rows_per_resource_defaults_to_no_cap(mod, monkeypatch):
    plain = _step_counts(_materialize(mod, monkeypatch))
    unset = _step_counts(_materialize(mod, monkeypatch, max_rows_per_resource=None))
    assert plain == unset


# --- Shared helpers (unit-level) ---------------------------------------------------


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}
