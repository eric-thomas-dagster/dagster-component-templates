"""receipts: maintenance (python mode): `rewrite_candidates` in the summary and `prune` (dead weight: flag / exclude).

Real chromadb, a real ContextReceiptStore (loaded via importlib, as test_receipts_feedback does), real Dagster; only the embedding call is
faked. The footgun guards of prune.mode=exclude are MUTATION-CHECKED: each is removed from a copy of component.py and the scenario that
protects it must then delete chunks it should not have.

Chunk ids of the default 3-paragraph document: d1_0, d1_1, d1_2 (source id d1)."""
import importlib.util
import pathlib
import time

import pytest

from .conftest import load_component_module, requires_chromadb
from .test_receipts_feedback import STORE_PATH, build, kb_md, metas, serve  # noqa: F401  (store fixture below uses the same loader)

pytestmark = requires_chromadb

NEW_DOC = "Fourth paragraph about invoices.\n\nFifth paragraph about taxes."


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def store(tmp_path):
    spec = importlib.util.spec_from_file_location("receipt_store_for_maintenance_tests", STORE_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def mutated(tmp_path, old, new, name):
    """component.py with one guard removed, loaded as a separate module."""
    src = pathlib.Path(load_component_module().__file__).read_text()
    assert src.count(old) == 1, old
    path = tmp_path / f"{name}.py"
    path.write_text(src.replace(old, new))
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# the three guards, as literal source lines
G_TRAFFIC = ('judged = receipts_for_prune >= int(pr["never_retrieved_after_receipts"])', "judged = True")
G_AGE = ("datetime.fromisoformat(t) <= cutoff for t in stamped", "True for t in stamped")
G_RECIPE = ('int((by_cv.get(recipe) or {}).get("receipts", 0)) if recipe is not None else receipts_total', "receipts_total")

PRUNE = {"never_retrieved_after_receipts": 5, "mode": "exclude", "min_age_days": 1e-6}


def aged_build(mod, tmp_path, store):
    """Build once (stamps first_seen_at), let the chunks age past min_age_days, then serve and build again."""
    build(mod, tmp_path, store, receipts={"prune": {**PRUNE, "mode": "flag"}})
    time.sleep(0.25)


# ── 1. rewrite candidates ────────────────────────────────────────────────────────────────────────────────

def test_rewrite_candidates_are_reported_with_source_ids_and_independent_of_quarantine(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_1", 6, outcome=-1)       # retrieved and rated enough, rated badly
    serve(store, tmp_path, "d1_2", 3, outcome=-1)       # bad, but below min_retrievals=5
    serve(store, tmp_path, "d1_0", 6, outcome=+1)
    result, _ = build(mod, tmp_path, store)             # no quarantine configured at all
    s = kb_md(result)["receipts_feedback"].value
    assert s["rewrite_candidates"] == [{"chunk_id": "d1_1", "source_id": "d1", "retrievals": 6, "rated": 6, "mean_outcome": -1.0, "cited": 0}]
    assert s["rewrite_below"] == -0.3 and s["worst"][0]["chunk_id"] == "d1_1" and s["best"][0]["chunk_id"] == "d1_0"   # existing keys kept
    # still listed when quarantine EXCLUDES it (an excluded chunk is exactly what needs rewriting)
    result, _ = build(mod, tmp_path, store, receipts={"quarantine": {"mode": "exclude"}})
    assert "d1_1" not in metas(tmp_path)
    assert [c["chunk_id"] for c in kb_md(result)["receipts_feedback"].value["rewrite_candidates"]] == ["d1_1"]
    # rewrite_below is its own threshold (-1.0 is the worst possible mean, so a stricter bar than d1_1's -1.0 mean cannot exist; use -0.99)
    result, _ = build(mod, tmp_path, store, receipts={"rewrite_below": -0.99})
    assert [c["chunk_id"] for c in kb_md(result)["receipts_feedback"].value["rewrite_candidates"]] == ["d1_1"]
    serve(store, tmp_path, "d1_1", 6, outcome=+1)                      # now 12 ratings, mean 0.0
    result, _ = build(mod, tmp_path, store, receipts={"rewrite_below": -0.3})
    assert kb_md(result)["receipts_feedback"].value["rewrite_candidates"] == []


def test_rewrite_candidates_equal_the_stores_maintenance_report_on_the_same_events(mod, tmp_path, store):
    """Parity with resources/context_receipt_store fold_maintenance_report (loaded via importlib, never imported by the component)."""
    build(mod, tmp_path, store)
    for chunk, n, v in [("d1_0", 7, -1), ("d1_1", 9, -1), ("d1_2", 6, +1)]:
        serve(store, tmp_path, chunk, n, outcome=v)
    serve(store, tmp_path, "d1_0", 2, outcome=+1)       # d1_0 mixed: 7 bad + 2 good -> mean -0.556
    result, asset_def = build(mod, tmp_path, store, receipts={"rewrite_below": -0.3})
    mine = kb_md(result)["receipts_feedback"].value
    since = "1970-01-01T00:00:00+00:00"
    recipe = mine["scoped_to_recipe"]
    theirs = store.maintenance_report("kb", since=since, code_version=recipe, min_retrievals=5, min_outcomes=3, bad_below=-0.3)
    key = lambda r: (r.get("chunk_id") or r.get("item_id"), r["retrievals"], r["rated"], r["mean_outcome"], r["cited"])  # noqa: E731
    assert [key(r) for r in mine["rewrite_candidates"]] == [key(r) for r in theirs["rewrite_candidates"]]
    assert len(mine["rewrite_candidates"]) == 2 and mine["rewrite_candidates"][0]["chunk_id"] == "d1_1"       # worst first


# ── 2. prune ─────────────────────────────────────────────────────────────────────────────────────────────

def test_prune_flag_marks_dead_weight_and_keeps_it_indexed(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)                    # 6 receipts in the window; d1_1 / d1_2 never retrieved
    result, _ = build(mod, tmp_path, store, receipts={"prune": {**PRUNE, "mode": "flag"}})
    m = metas(tmp_path)
    assert {c: m[c]["feedback_dead_weight"] for c in m} == {"d1_0": False, "d1_1": True, "d1_2": True}
    assert set(m) == {"d1_0", "d1_1", "d1_2"} and all(m[c]["feedback_first_seen_at"] for c in m)
    s = kb_md(result)["receipts_feedback"].value
    assert s["pruned"] == 2 and s["prune_mode"] == "flag" and s["prune_judged"] is True and s["pruned_ids"] == ["d1_1", "d1_2"]


def test_prune_flag_agrees_with_the_stores_dead_weight(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)
    result, _ = build(mod, tmp_path, store, receipts={"scope_to_recipe": False, "prune": {**PRUNE, "mode": "flag"}})
    flagged = sorted(c for c, v in metas(tmp_path).items() if v["feedback_dead_weight"])
    theirs = store.maintenance_report("kb", since="1970-01-01T00:00:00+00:00", known_item_ids=list(metas(tmp_path)), dead_after_receipts=5)
    assert flagged == theirs["dead_weight"] == ["d1_1", "d1_2"]


def test_prune_exclude_removes_dead_chunks_until_the_window_stops_qualifying(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)
    result, _ = build(mod, tmp_path, store, receipts={"prune": PRUNE})
    assert sorted(metas(tmp_path)) == ["d1_0"]
    s = kb_md(result)["receipts_feedback"].value
    assert s["pruned_ids"] == ["d1_1", "d1_2"] and "feedback_dead_weight" not in metas(tmp_path)["d1_0"]
    # the traffic ages out of the window: absence is no longer evidence, so the next build re-adds the chunks by itself
    time.sleep(0.3)
    result, _ = build(mod, tmp_path, store, receipts={"window_days": 1e-6, "prune": PRUNE})
    assert sorted(metas(tmp_path)) == ["d1_0", "d1_1", "d1_2"]
    assert kb_md(result)["receipts_feedback"].value["prune_judged"] is False


def test_prune_exclude_works_with_upsert_sync_and_alongside_quarantine(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6, outcome=-1)        # d1_0 is bad (quarantined), d1_1 / d1_2 are dead
    result, _ = build(mod, tmp_path, store, sync_mode="upsert", receipts={"prune": PRUNE, "quarantine": {"mode": "exclude"}})
    assert sorted(metas(tmp_path)) == []                 # both purges delete by id, so upsert sync is no obstacle


def test_prune_key_is_removed_again_when_prune_is_switched_off(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    build(mod, tmp_path, store)                          # no prune: Chroma merges metadata, so the stale keys must be cleared
    m = metas(tmp_path)["d1_0"]
    assert "feedback_dead_weight" not in m and "feedback_first_seen_at" not in m


# ── footguns (each one mutation-checked) ─────────────────────────────────────────────────────────────────

def scenario_not_enough_traffic(mod, tmp_path, store):
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 3)                    # 3 receipts < never_retrieved_after_receipts=5
    build(mod, tmp_path, store, receipts={"prune": PRUNE})
    return sorted(metas(tmp_path))


def scenario_new_document_next_to_old_dead_chunks(mod, tmp_path, store):
    from .test_receipt_contract import DOC
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)
    build(mod, tmp_path, store, rows=[("d1", DOC), ("d2", NEW_DOC)], receipts={"prune": PRUNE})
    return sorted(metas(tmp_path))


def scenario_recipe_change(mod, tmp_path, store):
    """All traffic was recorded under an older recipe: the current recipe has seen no receipts, so zero retrievals prove nothing."""
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 8, cv="older-recipe")
    build(mod, tmp_path, store, receipts={"prune": PRUNE})
    return sorted(metas(tmp_path))


ALL = ["d1_0", "d1_1", "d1_2"]


def fresh(tmp_path, name):
    p = tmp_path / name
    p.mkdir()
    return p


def make_store(tmp_path):
    spec = importlib.util.spec_from_file_location("receipt_store_mut", STORE_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def test_exclude_never_fires_below_the_receipt_threshold(mod, tmp_path, store):
    assert scenario_not_enough_traffic(mod, tmp_path, store) == ALL
    bad = mutated(tmp_path, *G_TRAFFIC, "no_traffic_guard")
    p = fresh(tmp_path, "m1")
    assert scenario_not_enough_traffic(bad, p, make_store(p)) == ["d1_0"]            # mutation: the guard is what saved d1_1 / d1_2


def test_exclude_never_deletes_chunks_younger_than_min_age(mod, tmp_path, store):
    build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)
    build(mod, tmp_path, store, receipts={"prune": {"never_retrieved_after_receipts": 5, "mode": "exclude"}})   # default min_age = window
    assert sorted(metas(tmp_path)) == ALL                # index predates prune: no first_seen_at, so every chunk counts as new
    p = fresh(tmp_path, "n1")
    got = scenario_new_document_next_to_old_dead_chunks(mod, p, make_store(p))
    assert got == ["d1_0", "d2_0", "d2_1"]               # old dead chunks go; the new document's chunks (first seen now) are protected
    bad = mutated(tmp_path, *G_AGE, "no_age_guard")
    p = fresh(tmp_path, "m2")
    assert scenario_new_document_next_to_old_dead_chunks(bad, p, make_store(p)) == ["d1_0"]    # mutation: new chunks wiped too


def test_exclude_does_not_mistake_a_recipe_change_for_no_traffic(mod, tmp_path, store):
    assert scenario_recipe_change(mod, tmp_path, store) == ALL
    bad = mutated(tmp_path, *G_RECIPE, "no_recipe_scope")
    p = fresh(tmp_path, "m3")
    assert scenario_recipe_change(bad, p, make_store(p)) == []                       # mutation: the whole index is wiped


def test_a_pruned_chunk_comes_back_on_probation_and_is_protected_for_min_age(mod, tmp_path, store):
    """Excluded chunks leave the index, so they lose their first_seen_at. Re-added later they are new again: protected for min_age_days,
    then judged afresh -- an excluded chunk is never banned forever, and never flaps in and out on consecutive builds."""
    aged_build(mod, tmp_path, store)
    serve(store, tmp_path, "d1_0", 6)
    build(mod, tmp_path, store, receipts={"prune": PRUNE})
    assert sorted(metas(tmp_path)) == ["d1_0"]
    build(mod, tmp_path, store, receipts={"prune": {**PRUNE, "min_age_days": 30}})      # re-added by the next build ... (window still qualifies)
    assert sorted(metas(tmp_path)) == ALL                                                # ... protected, because it is new again
    time.sleep(0.25)
    build(mod, tmp_path, store, receipts={"prune": PRUNE})
    assert sorted(metas(tmp_path)) == ["d1_0"]                                           # a full probation later it is judged again


# ── config ───────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad,msg", [
    ({"prune": {"mode": "delete"}}, "flag' or 'exclude"),
    ({"prune": {"nope": 1}}, "receipts.prune must be"),
    ({"prune": {"never_retrieved_after_receipts": 0}}, ">= 1"),
    ({"prune": {"min_age_days": -1}}, "min_age_days must be >= 0"),
    ({"rewrite_below": 2}, r"rewrite_below must be in \[-1, 1\]"),
])
def test_invalid_maintenance_config_is_rejected(mod, tmp_path, bad, msg):
    from .test_receipt_contract import _component
    with pytest.raises(ValueError, match=msg):
        _component(mod, tmp_path, receipts={"resource_key": "context_receipts", **bad}).build_defs(None)


def test_prune_needs_the_chromadb_provider_and_defaults_min_age_to_the_window(mod, tmp_path):
    from .test_receipt_contract import _component
    steps = [{"id": "chunks", "op": "chunk"}, {"id": "embedded", "op": "embed", "provider": "litellm", "model": "m"},
             {"id": "indexed", "op": "write_vector_store", "provider": "qdrant", "collection_name": "c"}]
    with pytest.raises(ValueError, match="chromadb provider only"):
        _component(mod, tmp_path, steps=steps, receipts={"resource_key": "k", "prune": {"mode": "flag"}}).build_defs(None)
    cfg = mod._validate_receipts({"resource_key": "k", "window_days": 9, "prune": {}}, [{"op": "write_vector_store"}], "python")
    assert cfg["prune"] == {"never_retrieved_after_receipts": 200, "mode": "flag", "min_age_days": 9.0} and cfg["rewrite_below"] == -0.3
