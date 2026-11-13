"""hash_columns/on_schema_change/exclude_columns/max_rows_per_resource for
TavilyIngestionComponent. `dlt.pipeline` is faked (see conftest.py) and `.run()` never
consumes the resource generator, so the real Tavily API is never called here -- these
check that the resource gained the expected pipe step / schema_contract, same convention
as activecampaign_ingestion/tests/test_dlt_advanced_options.py.
"""
import pathlib
import shutil

import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_tavily_search, load_component_module, requires_duckdb


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


# --- write_disposition: resource hints (fake-dlt, real .apply_hints()) -----------------
# `_materialize` above never consumes the resource generator (install_fake_dlt's
# default consume=False), so these only check that the REAL dlt resource gained the
# expected write_disposition/primary_key hint -- no Tavily request is made.


def test_default_write_disposition_is_replace(mod, monkeypatch):
    resource = _materialize(mod, monkeypatch)
    assert resource.write_disposition == "replace"


def test_merge_write_disposition_sets_primary_key(mod, monkeypatch):
    resource = _materialize(mod, monkeypatch, write_disposition="merge")
    assert resource.write_disposition == "merge"
    assert resource.compute_table_schema()["columns"]["doc_id"]["primary_key"] is True


def test_append_write_disposition_is_explicit_opt_in(mod, monkeypatch):
    resource = _materialize(mod, monkeypatch, write_disposition="append")
    assert resource.write_disposition == "append"


# --- _dedupe_by_doc_id: pure-function safety net ---------------------------------------


def test_dedupe_by_doc_id_keeps_latest_updated_at(mod):
    df = pd.DataFrame([
        {"doc_id": "r1", "updated_at": "2024-01-01T00:00:00Z", "content": "old"},
        {"doc_id": "r1", "updated_at": "2024-02-01T00:00:00Z", "content": "new"},
        {"doc_id": "r2", "updated_at": "2024-01-01T00:00:00Z", "content": "b"},
    ])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["r1", "content"] == "new"


def test_dedupe_by_doc_id_no_doc_id_column_is_a_no_op(mod):
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_doc_id_empty_dataframe_is_a_no_op(mod):
    df = pd.DataFrame(columns=["doc_id", "updated_at"])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 0


def test_dedupe_by_doc_id_without_updated_at_column_still_dedupes(mod):
    df = pd.DataFrame([{"doc_id": "r1", "content": "a"}, {"doc_id": "r1", "content": "b"}])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 1


# --- real dlt + real (file-backed) duckdb: the actual regression this was fixing ------
# Faking dlt.pipeline() (as every test above does) can't prove accumulation is actually
# prevented at the destination -- it never persists anything between two separate
# FakePipeline instances. This test uses the REAL dlt.pipeline()/duckdb destination (the
# component's own default when `destination` is unset) so a real regression of the exact
# bug this fixes -- row_count silently doubling on every rerun -- would be caught.
#
# dlt also keeps its OWN local pipeline-sync state in ~/.dlt/pipelines/<pipeline_name>/ --
# a GLOBAL directory, independent of cwd/tmp_path -- so this uses a pipeline_name unique
# to this test and tears down that global directory before running, so no prior run of
# this file (in any order, on any host that has run it before) can see stale state.


def _reset_dlt_pipeline_dir(pipeline_name: str) -> None:
    shutil.rmtree(pathlib.Path.home() / ".dlt" / "pipelines" / pipeline_name, ignore_errors=True)


def _two_result_responder(url, headers, payload):
    q = payload["query"]
    return {
        "query": q,
        "results": [
            {"title": f"{q} result 1", "url": "https://a.example", "content": "snippet one", "score": 0.91, "id": "r1"},
            {"title": f"{q} result 2", "url": "https://b.example", "content": "snippet two", "score": 0.42, "id": "r2"},
        ],
    }


@requires_duckdb
def test_rerun_with_default_write_disposition_does_not_duplicate_rows(mod, monkeypatch, tmp_path):
    _reset_dlt_pipeline_dir("tavily_rerun_test_asset_pipeline")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    install_fake_tavily_search(monkeypatch, _two_result_responder)

    def _build_and_materialize():
        component = mod.TavilyIngestionComponent(asset_name="tavily_rerun_test_asset", queries="dagster")
        asset_def = component.build_defs(context=None).assets[0]
        return dg.materialize([asset_def])

    result1 = _build_and_materialize()
    assert result1.success
    md1 = result1.asset_materializations_for_node("tavily_rerun_test_asset")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["write_disposition"].value == "replace"

    result2 = _build_and_materialize()
    assert result2.success
    md2 = result2.asset_materializations_for_node("tavily_rerun_test_asset")[0].metadata
    assert md2["row_count"].value == 2  # NOT 4 -- this is the exact bug being fixed
    assert md2["duplicate_rows_removed"].value == 0


@requires_duckdb
def test_rerun_with_append_write_disposition_accumulates_but_dedupe_safety_net_catches_it(mod, monkeypatch, tmp_path):
    _reset_dlt_pipeline_dir("tavily_rerun_append_test_asset_pipeline")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    install_fake_tavily_search(monkeypatch, _two_result_responder)

    def _build_and_materialize():
        component = mod.TavilyIngestionComponent(
            asset_name="tavily_rerun_append_test_asset", queries="dagster", write_disposition="append",
        )
        asset_def = component.build_defs(context=None).assets[0]
        return dg.materialize([asset_def])

    result1 = _build_and_materialize()
    assert result1.success
    md1 = result1.asset_materializations_for_node("tavily_rerun_append_test_asset")[0].metadata
    assert md1["row_count"].value == 2

    result2 = _build_and_materialize()
    assert result2.success
    md2 = result2.asset_materializations_for_node("tavily_rerun_append_test_asset")[0].metadata
    # The destination table itself now holds 4 rows (2 real appended loads of the same
    # 2 doc_ids) -- but the dedupe safety net, applied after the SELECT * readback and
    # independent of write_disposition, collapses them back to 2 distinct doc_ids before
    # the DataFrame is returned.
    assert md2["row_count"].value == 2
    assert md2["duplicate_rows_removed"].value == 2
