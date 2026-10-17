"""receipts: -- real retrieval receipts (a real ContextReceiptStore) shaping the next real build (real chromadb + Dagster).

Only the embedding call is faked. Traffic is recorded the way a gateway would: items built from the metadata the pipeline stamped
on the chunks it wrote."""
import importlib.util
import time
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb
from .test_receipt_contract import DOC, PARAGRAPHS, _collection, _component, _fake_embedding

pytestmark = requires_chromadb

STORE_PATH = Path(__file__).resolve().parents[4] / "resources" / "context_receipt_store" / "component.py"
KEY = "context_receipts"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def store(tmp_path):
    spec = importlib.util.spec_from_file_location("receipt_store_for_pipeline_tests", STORE_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def build(mod, tmp_path, store, receipts=None, rows=None, instance=None, raise_on_error=True, **kw):
    comp = _component(mod, tmp_path, receipts={"resource_key": KEY, **(receipts or {})}, **kw)
    asset_def = list(comp.build_defs(None).assets)[0]
    df = pd.DataFrame(rows or [("d1", DOC)], columns=["id", "body"])

    @dg.asset(name="raw")
    def raw():
        return df

    with patch("litellm.embedding", side_effect=_fake_embedding):
        return dg.materialize([asset_def, raw], resources={KEY: store}, instance=instance or dg.DagsterInstance.ephemeral(),
                              raise_on_error=raise_on_error), asset_def


def kb_md(result):
    """Metadata of the knowledge-base asset's materialization (the run also materializes the `raw` source first)."""
    return next(e.materialization.metadata for e in result.get_asset_materialization_events()
                if e.materialization.asset_key == dg.AssetKey("kb"))


def metas(tmp_path):
    got = _collection(tmp_path).get(include=["metadatas"])
    return dict(zip(got["ids"], got["metadatas"]))


def serve(store, tmp_path, chunk, n, outcome=None, cv=None):
    """Record n retrievals of `chunk` the way a gateway would, each followed by `outcome` (+1 / -1)."""
    meta = metas(tmp_path)[chunk]
    for _ in range(n):
        r = store.record_retrieval(context_name="kb_ctx", items=[{
            "item_id": chunk, "source_asset_key": "kb", "source_version": meta["source_version"],
            "source_code_version": cv or meta["source_code_version"], "rank": 1}])
        if outcome is not None:
            store.record_outcome(r, signal="thumbs_up" if outcome > 0 else "thumbs_down")


def test_first_build_stamps_zeroed_feedback_and_reports_it(mod, tmp_path, store):
    result, _ = build(mod, tmp_path, store)
    m = metas(tmp_path)["d1_0"]
    assert m["feedback_retrievals"] == 0 and m["feedback_rated"] == 0 and m["feedback_mean_outcome"] == 0.0
    md = kb_md(result)
    s = md["receipts_feedback"].value
    assert s["chunks_in_build"] == 3 and s["chunks_with_feedback"] == 0 and s["quarantined"] == 0
    assert md["receipts_quarantined"].value == 0


def test_real_traffic_flows_back_into_the_next_build(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)       # users keep rating this chunk badly
    serve(store, tmp_path, "d1_0", 6, outcome=+1)
    result, _ = build(mod, tmp_path, store)             # next build, same recipe
    m = metas(tmp_path)
    assert (m["d1_1"]["feedback_retrievals"], m["d1_1"]["feedback_rated"], m["d1_1"]["feedback_mean_outcome"]) == (6, 6, -1.0)
    assert m["d1_0"]["feedback_mean_outcome"] == 1.0 and m["d1_2"]["feedback_retrievals"] == 0
    assert m["d1_1"]["feedback_prior"] < 0 < m["d1_0"]["feedback_prior"]        # priors were refreshed from the receipts
    s = kb_md(result)["receipts_feedback"].value
    assert s["worst"][0]["chunk_id"] == "d1_1" and s["best"][0]["chunk_id"] == "d1_0"


def test_quarantine_flag_marks_the_bad_chunk_but_keeps_it_indexed(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)
    serve(store, tmp_path, "d1_0", 6, outcome=+1)
    result, _ = build(mod, tmp_path, store, receipts={"quarantine": {"mean_outcome_below": -0.3, "mode": "flag"}})
    m = metas(tmp_path)
    assert m["d1_1"]["feedback_quarantined"] is True and m["d1_0"]["feedback_quarantined"] is False
    assert set(m) == {"d1_0", "d1_1", "d1_2"}
    assert kb_md(result)["receipts_quarantined"].value == 1


def test_quarantine_exclude_removes_the_chunk_then_probation_brings_it_back(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)
    result, _ = build(mod, tmp_path, store, receipts={"quarantine": {"mode": "exclude"}})
    assert sorted(metas(tmp_path)) == ["d1_0", "d1_2"]                              # left out of the index
    s = kb_md(result)["receipts_feedback"].value
    assert s["quarantined_ids"] == ["d1_1"] and s["quarantine_mode"] == "exclude"
    # an excluded chunk gets no new receipts, so it must not be banned forever: once its old receipts age out of the window
    # it is judged afresh (probation)
    time.sleep(0.3)
    build(mod, tmp_path, store, receipts={"quarantine": {"mode": "exclude"}, "window_days": 1e-6})
    assert sorted(metas(tmp_path)) == ["d1_0", "d1_1", "d1_2"]


def test_not_enough_evidence_means_no_quarantine(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 3, outcome=-1)                                    # below min_retrievals=5
    build(mod, tmp_path, store, receipts={"quarantine": {"mode": "exclude"}})
    assert "d1_1" in metas(tmp_path)
    serve(store, tmp_path, "d1_2", 8)                                                # plenty of retrievals but none rated
    build(mod, tmp_path, store, receipts={"quarantine": {"mode": "exclude"}})
    assert "d1_2" in metas(tmp_path)


def test_a_recipe_change_starts_fresh_unless_scoping_is_off(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)
    other_recipe = [{"id": "chunks", "op": "chunk", "strategy": "recursive", "chunk_size": 40, "chunk_overlap": 0},
                    {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-1"},
                    {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "connection_string": str(tmp_path / "chroma"),
                     "collection_name": "kb_test"}]
    build(mod, tmp_path, store, steps=other_recipe)
    assert metas(tmp_path)["d1_1"]["feedback_retrievals"] == 0                       # old feedback described different chunks
    build(mod, tmp_path, store, steps=other_recipe, receipts={"scope_to_recipe": False})
    assert metas(tmp_path)["d1_1"]["feedback_retrievals"] == 6


def test_dead_weight_and_exclusion_reasons_are_reported(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 4)
    meta = metas(tmp_path)["d1_0"]
    store.record_retrieval(context_name="kb_ctx", items=[], excluded=[{
        "item_id": "d1_2", "reason": "stale", "source_asset_key": "kb", "source_code_version": meta["source_code_version"]}])
    result, _ = build(mod, tmp_path, store, receipts={"dead_weight_min_receipts": 3})
    s = kb_md(result)["receipts_feedback"].value
    assert s["never_retrieved"] == 2 and s["exclusion_reasons"] == {"stale": 1}      # d1_1 and d1_2 never retrieved
    result, _ = build(mod, tmp_path, store, receipts={"dead_weight_min_receipts": 500})
    assert kb_md(result)["receipts_feedback"].value["never_retrieved"] is None


def test_stamp_off_keeps_feedback_out_of_the_index_but_still_quarantines(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)
    build(mod, tmp_path, store, receipts={"stamp": False, "quarantine": {"mode": "exclude"}})
    m = metas(tmp_path)
    assert "d1_1" not in m and not any(k.startswith("feedback_") for k in m["d1_0"])


def recipe_check(result):
    (ev,) = result.get_asset_check_evaluations()
    return ev


def test_recipe_outcomes_check_flags_a_recipe_that_does_worse_than_an_earlier_one(mod, tmp_path, store):
    r0, asset_def = build(mod, tmp_path, store)
    current = asset_def.code_versions_by_key[dg.AssetKey("kb")]
    serve(store, tmp_path, "d1_0", 10, outcome=+1, cv="older-recipe")               # the earlier recipe: users were happy
    serve(store, tmp_path, "d1_0", 10, outcome=-1)                                   # the current recipe: they are not
    result, _ = build(mod, tmp_path, store, receipts={"regression_check": {"min_outcomes": 10, "max_drop": 0.15}})
    ev = recipe_check(result)
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN and ev.metadata["status"].value == "regressed"
    assert ev.metadata["best_previous_version"].value == "older-recipe" and ev.metadata["drop"].value == pytest.approx(2.0)
    assert "rolling back" in ev.description
    result, _ = build(mod, tmp_path, store, receipts={"regression_check": {"min_outcomes": 10, "max_drop": 5, "severity": "ERROR"}})
    assert recipe_check(result).passed                                               # drop within the allowance


def test_recipe_outcomes_check_is_honest_about_missing_evidence(mod, tmp_path, store):
    result, _ = build(mod, tmp_path, store, receipts={"regression_check": {}})
    ev = recipe_check(result)
    assert ev.passed and ev.metadata["status"].value == "insufficient_evidence"
    serve(store, tmp_path, "d1_0", 12, outcome=+1)                                   # current recipe rated, but nothing to compare to
    result, _ = build(mod, tmp_path, store, receipts={"regression_check": {}})
    assert recipe_check(result).metadata["status"].value == "no_baseline" and recipe_check(result).passed


@pytest.mark.parametrize("bad,msg", [
    ({"nope": 1}, "unknown field"),
    ({"resource_key": ""}, "resource_key is required"),
    ({"window_days": 0}, "window_days"),
    ({"quarantine": {"mode": "delete"}}, "flag' or 'exclude"),
    ({"quarantine": {"mean_outcome_below": 3}}, r"\[-1, 1\]"),
    ({"regression_check": {"severity": "LOUD"}}, "WARN or ERROR"),
])
def test_invalid_config_is_rejected(mod, tmp_path, bad, msg):
    comp = _component(mod, tmp_path, receipts={"resource_key": KEY, **bad})
    with pytest.raises(ValueError, match=msg):
        comp.build_defs(None)


def test_exclude_deletes_by_id_so_it_works_with_every_sync_mode(mod, tmp_path, store):
    build(mod, tmp_path, store, sync_mode="upsert")
    serve(store, tmp_path, "d1_1", 6, outcome=-1)
    build(mod, tmp_path, store, sync_mode="upsert", receipts={"quarantine": {"mode": "exclude"}})
    assert sorted(metas(tmp_path)) == ["d1_0", "d1_2"]          # upsert never deletes on its own; the quarantine purge does


def test_a_quarantined_chunk_that_was_its_documents_only_chunk_is_still_removed(mod, tmp_path, store):
    rows = [("solo", "A single short paragraph."), ("multi", DOC)]
    build(mod, tmp_path, store, rows=rows)
    serve(store, tmp_path, "solo_0", 6, outcome=-1)
    build(mod, tmp_path, store, rows=rows, receipts={"quarantine": {"mode": "exclude"}})
    assert "solo_0" not in metas(tmp_path) and {"multi_0", "multi_1", "multi_2"} <= set(metas(tmp_path))   # sync alone would have missed it


def test_exclude_needs_a_provider_that_can_delete_by_id_and_sql_mode_receipts_are_bigquery_only(mod, tmp_path):
    steps = [{"id": "chunks", "op": "chunk"}, {"id": "embedded", "op": "embed", "provider": "litellm", "model": "m"},
             {"id": "indexed", "op": "write_vector_store", "provider": "qdrant", "collection_name": "c"}]
    comp = _component(mod, tmp_path, steps=steps, receipts={"resource_key": KEY, "quarantine": {"mode": "exclude"}})
    with pytest.raises(ValueError, match="chromadb provider only"):
        comp.build_defs(None)
    _component(mod, tmp_path, steps=steps, receipts={"resource_key": KEY, "quarantine": {"mode": "flag"}}).build_defs(None)  # flag is fine anywhere
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", source={"kind": "warehouse_query", "resource_key": "wh", "sql": "select 1"}, id_column="id", text_column="body",
        execution_mode="sql", sql_dialect="snowflake_cortex", output_table="t", steps=[{"id": "c", "op": "chunk"}],
        receipts={"resource_key": KEY})
    with pytest.raises(ValueError, match="bigquery' only"):
        comp.build_defs(None)


def test_a_resource_that_is_not_a_receipt_store_fails_clearly(mod, tmp_path):
    result, _ = build(mod, tmp_path, object(), raise_on_error=False)
    assert not result.success
    msg = " ".join(e.event_specific_data.error.message + (e.event_specific_data.error.cause.message if e.event_specific_data.error.cause else "")
                   for e in result.get_step_failure_events())
    assert "not a ContextReceiptStore resource" in msg


def test_omitting_receipts_changes_nothing(mod, tmp_path):
    comp = _component(mod, tmp_path)
    asset_def = list(comp.build_defs(None).assets)[0]
    assert KEY not in asset_def.required_resource_keys and not list(asset_def.check_specs)
