"""on_schema_change/exclude_columns/max_rows_per_resource unit-level checks.

This component's conftest.py fakes `rest_api_source` itself (not just
`dlt.pipeline`) with a stand-in that doesn't support `.resources` access -- a
pre-existing gap affecting any field that touches resource objects (hash_columns
included, had one of these tests tried to materialize with it set). So these check
the shared helper functions directly, and that invalid config is still rejected at
construction time, rather than materializing end-to-end.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        mod.LatticeIngestionComponent(**dict(asset_name="test_asset", api_key="lattice_key_abc123"), on_schema_change="bogus")


def test_exclude_columns_applied_before_hash_columns(mod):
    exclude_map = mod._make_exclude_columns_map(["email"])
    hash_map = mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}
