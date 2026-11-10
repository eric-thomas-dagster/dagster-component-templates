"""on_schema_change/exclude_columns/max_rows_per_resource.

NOTE: this component has a REAL, PRE-EXISTING bug unrelated to this rollout -- its
`client.auth` config uses `"type": "oauth1"`, which dlt's rest_api_source does not
support (verified: `rest_api_source(config)` raises `DictValidationException` on
construction, not just at materialize time, so this would also fail with real
credentials in production). That means `component.build_defs(...)` followed by
`dg.materialize(...)` cannot succeed for this component at all today, regardless of
these new fields. These tests therefore verify field presence/defaults and the shared
helper functions directly, unit-level, rather than exercising a real build+materialize
round-trip the way other components' test_dlt_advanced_options.py do.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        consumer_key="ck", consumer_secret="cs",
        access_token="at", access_token_secret="ats",
        account_ids="abc123",
    )
    kwargs.update(overrides)
    return mod.TwitterAdsIngestionComponent(**kwargs)


def test_on_schema_change_defaults_to_allow_all(mod):
    component = _make_component(mod)
    assert component.on_schema_change == "allow_all"


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        _make_component(mod, on_schema_change="bogus")


def test_exclude_columns_defaults_to_none(mod):
    component = _make_component(mod)
    assert component.exclude_columns is None


def test_max_rows_per_resource_defaults_to_none(mod):
    component = _make_component(mod)
    assert component.max_rows_per_resource is None


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
