"""on_schema_change/exclude_columns/max_rows_per_resource: this component's conftest
fakes rest_api_source() as a bare placeholder object (no .resources support) -- the
same reason hash_columns has no end-to-end test here either (see conftest.py). These
verify the shared helper functions directly and that invalid config is rejected, which
is the same test depth hash_columns itself has in this component.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


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


def test_on_schema_change_rejects_invalid_value_at_construction(mod):
    with pytest.raises(Exception):
        mod.FreshsalesIngestionComponent(asset_name="x", domain="widgetz", api_key="abc123", on_schema_change="bogus")
