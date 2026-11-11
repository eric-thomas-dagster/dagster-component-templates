"""hash_columns/on_schema_change/exclude_columns/max_rows_per_resource for
TavilyIngestionComponent. `dlt.pipeline` is faked (see conftest.py) and `.run()` never
consumes the resource generator, so the real Tavily API is never called here -- these
check that the resource gained the expected pipe step / schema_contract, same convention
as activecampaign_ingestion/tests/test_dlt_advanced_options.py.
"""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, monkeypatch, **overrides):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    kwargs = dict(asset_name="test_asset", queries="dagster")
    kwargs.update(overrides)
    component = mod.TavilyIngestionComponent(**kwargs)
    defs = component.build_defs(context=None)
    dg.materialize(list(defs.assets))
    return fake_pipeline.run_calls[-1]


def _step_count(resource):
    return len(resource._pipe._steps)


# --- on_schema_change -----------------------------------------------------------


def test_on_schema_change_default_does_not_set_schema_contract(mod, monkeypatch):
    resource = _materialize(mod, monkeypatch)
    assert not resource.compute_table_schema().get("schema_contract")


def test_on_schema_change_block_all_sets_schema_contract(mod, monkeypatch):
    resource = _materialize(mod, monkeypatch, on_schema_change="block_all")
    contract = resource.compute_table_schema().get("schema_contract")
    assert contract == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        mod.TavilyIngestionComponent(asset_name="a", queries="q", on_schema_change="bogus")


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


# --- hash_columns -----------------------------------------------------------------


def test_hash_columns_adds_a_pipe_step(mod, monkeypatch):
    plain = _step_count(_materialize(mod, monkeypatch))
    hashed = _step_count(
        _materialize(mod, monkeypatch, hash_columns=["title"], hash_salt="s")
    )
    assert hashed == plain + 1


def test_hash_columns_without_salt_raises(mod, monkeypatch):
    with pytest.raises(Exception, match="hash_salt"):
        _materialize(mod, monkeypatch, hash_columns=["title"])


def test_make_hash_columns_map_hashes_listed_columns(mod):
    fn = mod._make_hash_columns_map(["title"], "salt")
    out = fn({"title": "hello", "url": "kept"})
    assert out["url"] == "kept"
    assert out["title"] != "hello"
    assert len(out["title"]) == 64  # sha256 hex digest


# --- exclude_columns --------------------------------------------------------------


def test_exclude_columns_adds_a_pipe_step(mod, monkeypatch):
    plain = _step_count(_materialize(mod, monkeypatch))
    excluded = _step_count(_materialize(mod, monkeypatch, exclude_columns=["score"]))
    assert excluded == plain + 1


def test_exclude_columns_applied_before_hash_columns(mod):
    exclude_map = mod._make_exclude_columns_map(["email"])
    hash_map = mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}


# --- max_rows_per_resource ---------------------------------------------------------


def test_max_rows_per_resource_adds_a_pipe_step(mod, monkeypatch):
    plain = _step_count(_materialize(mod, monkeypatch))
    capped = _step_count(_materialize(mod, monkeypatch, max_rows_per_resource=5))
    assert capped == plain + 1


def test_max_rows_per_resource_defaults_to_no_cap(mod, monkeypatch):
    plain = _step_count(_materialize(mod, monkeypatch))
    unset = _step_count(_materialize(mod, monkeypatch, max_rows_per_resource=None))
    assert plain == unset
