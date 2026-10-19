"""Recipe comparison by traffic split (`receipts.shadow`).

Real chromadb (a live index and a candidate index built by the REAL context_engineering_pipeline component with a
different code_version), a real ContextReceiptStore, real Dagster materialization. Only embedding / completion calls
are faked."""
import types
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module
from .test_receipts import (  # noqa: F401  (index is a fixture)
    DOCS, QUERY, ROOT, _comp, _fake_completion, _fake_embedding, _load, _meta, _out, _run, _store, _vec, index,
)

chromadb = pytest.importorskip("chromadb")


@pytest.fixture(scope="module")
def candidate_index(tmp_path_factory):
    """Same documents, same asset key (support_kb), different recipe: code_version recipe-8, smaller chunks."""
    base = tmp_path_factory.mktemp("kb2")
    cep = _load(ROOT / "assets/ai/context_engineering_pipeline/component.py", "cep_for_rag_shadow")
    kb = str(base / "kb")
    comp = cep.ContextEngineeringPipelineComponent(
        asset_name="support_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="body",
        code_version="recipe-8",
        steps=[
            {"id": "chunks", "op": "chunk", "strategy": "fixed", "chunk_size": 60, "chunk_overlap": 0, "min_chunk_chars": 10},
            {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-embed"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "connection_string": kb,
             "collection_name": "support_kb_v2", "sync_mode": "upsert"},
        ],
    )
    frame = pd.DataFrame(DOCS, columns=["doc_id", "body"])

    @dg.asset(name="raw_docs")
    def raw_docs():
        return frame

    with patch("litellm.embedding", side_effect=_fake_embedding):
        assert dg.materialize([raw_docs, *comp.build_defs(None).assets], instance=dg.DagsterInstance.ephemeral()).success
    return kb


@pytest.fixture()
def mod():
    return load_component_module()


def _shadow_comp(mod, index, candidate_index, shadow, steps=None, rerank=None, collection="support_kb_v2"):
    cfg = {"resource_key": "context_receipts", "shadow": shadow}
    if rerank:
        cfg["rerank"] = rerank
    comp = _comp(mod, index, cfg, steps)
    comp.retrievers["cand"] = {"kind": "chromadb", "path": candidate_index, "collection_name": collection}
    return comp


def _queries(n):
    return [f"{QUERY} variant {i}" for i in range(n)]


def _by_purpose(store, out):
    rs = [store.get_receipt(r) for r in out["receipt_id"]]
    return rs, [r["purpose"] for r in rs]


def _cvs(receipt):
    return {i["source_code_version"] for i in receipt["items"]}


# --- the split ---------------------------------------------------------------------------------

def test_fraction_is_roughly_honoured_and_each_recipe_keeps_its_own_provenance(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.3, "seed": 7, "label": "small-chunks"})
    res = _run(mod, comp, store, queries=_queries(200))
    rs, purposes = _by_purpose(store, _out(res))
    shadow_n = purposes.count("shadow:small-chunks")
    assert 40 <= shadow_n <= 80, shadow_n                                   # 30% of 200, binomial sd ~ 6.5
    assert _meta(res)["shadow_served"].value == shadow_n
    assert _meta(res)["shadow_fallbacks"].value == 0
    for r, p in zip(rs, purposes):
        assert _cvs(r) == ({"recipe-8"} if p == "shadow:small-chunks" else {"recipe-7"})
        assert {i["source_asset_key"] for i in r["items"]} == {"support_kb"}   # same asset, two recipes
        if p != "shadow:small-chunks":
            assert p is None


def test_outcomes_by_code_version_separates_the_two_recipes(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 3})
    out = _out(_run(mod, comp, store, queries=_queries(60)))
    rs, purposes = _by_purpose(store, out)
    for r, p in zip(rs, purposes):
        store.record_outcome(r["receipt_id"], signal="x", value=1.0 if p else -0.5, outcome_ref="u")
    by_cv = store.outcomes_by_code_version("support_kb")
    assert set(by_cv) == {"recipe-7", "recipe-8"}
    assert by_cv["recipe-8"]["outcomes"] == sum(1 for p in purposes if p) and by_cv["recipe-8"]["mean_outcome"] == 1.0
    assert by_cv["recipe-7"]["outcomes"] == sum(1 for p in purposes if not p) and by_cv["recipe-7"]["mean_outcome"] == -0.5


def test_candidate_is_actually_searched(mod, index, candidate_index, tmp_path):
    """The candidate index has different (smaller) chunks, so the served text differs from the live one."""
    _, store = _store(tmp_path)
    live = _out(_run(mod, _comp(mod, index, {"resource_key": "context_receipts"}), store))["sources"][0]
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1})
    shadow = _out(_run(mod, comp, store))["sources"][0]
    assert max(len(d["text"]) for d in shadow) <= 60 < max(len(d["text"]) for d in live)
    assert {d["metadata"]["source_code_version"] for d in shadow} == {"recipe-8"}


def test_fraction_one_serves_everything_from_the_candidate(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1})
    out = _out(_run(mod, comp, store, queries=_queries(10)))
    _, purposes = _by_purpose(store, out)
    assert purposes == ["shadow:cand"] * 10                                 # label defaults to the retriever name


def test_seeded_split_is_stable_per_query_and_seed_dependent(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    qs = _queries(80)

    def run(seed):
        comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.4, "seed": seed})
        return [bool(p) for p in _by_purpose(store, _out(_run(mod, comp, store, queries=qs)))[1]]
    a, b, c = run(11), run(11), run(12)
    assert a == b                                                           # same seed, same queries -> same split
    assert a != c                                                           # a different seed splits differently
    # order independence: the decision is a function of (seed, query), not of position
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.4, "seed": 11})
    rev = [bool(p) for p in _by_purpose(store, _out(_run(mod, comp, store, queries=qs[::-1])))[1]]
    assert rev == a[::-1]


def test_unseeded_split_is_random_across_runs(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    qs = _queries(60)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5})
    a = [bool(p) for p in _by_purpose(store, _out(_run(mod, comp, store, queries=qs)))[1]]
    b = [bool(p) for p in _by_purpose(store, _out(_run(mod, comp, store, queries=qs)))[1]]
    assert a != b


def test_hybrid_search_is_split_too(mod, index, candidate_index, tmp_path, monkeypatch):
    try:
        import rank_bm25  # noqa: F401
    except ImportError:
        fake = types.ModuleType("rank_bm25")

        class BM25Okapi:
            def __init__(self, corpus):
                self.corpus = corpus

            def get_scores(self, q):
                return [float(sum(w in doc for w in q)) for doc in self.corpus]
        fake.BM25Okapi = BM25Okapi
        monkeypatch.setitem(__import__("sys").modules, "rank_bm25", fake)
    _, store = _store(tmp_path)
    steps = [{"id": "found", "op": "hybrid_search", "retriever": "kb", "top_k": 2},
             {"id": "ans", "op": "generate", "source": "found", "model": "gpt-4o-mini"}]
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1}, steps=steps)
    rid = _out(_run(mod, comp, store))["receipt_id"][0]
    r = store.get_receipt(rid)
    assert r["purpose"] == "shadow:cand" and _cvs(r) == {"recipe-8"}


def test_code_version_override_when_the_candidate_chunks_share_the_live_version(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1, "code_version_override": "exp-A"})
    r = store.get_receipt(_out(_run(mod, comp, store))["receipt_id"][0])
    assert _cvs(r) == {"exp-A"}                                             # override wins for shadow-served receipts
    live = _comp(mod, index, {"resource_key": "context_receipts"})
    assert _cvs(store.get_receipt(_out(_run(mod, live, store))["receipt_id"][0])) == {"recipe-7"}


# --- guard rails -------------------------------------------------------------------------------

def test_candidate_failure_falls_back_to_live_and_never_fails_the_query(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1}, collection="does_not_exist")
    res = _run(mod, comp, store, queries=_queries(5))
    out = _out(res)
    rs, purposes = _by_purpose(store, out)
    assert purposes == [None] * 5 and all(_cvs(r) == {"recipe-7"} for r in rs)   # served (and recorded) as live
    assert all(len(s) == 2 for s in out["sources"]) and all(out["answer"])
    md = _meta(res)
    assert md["shadow_served"].value == 0 and md["shadow_fallbacks"].value == 5


def test_candidate_embedding_failure_also_falls_back(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1})
    comp.retrievers["cand"]["embedding"] = {"provider": "candidate-embedder"}

    def embed(cfg, q):
        if (cfg or {}).get("provider") == "candidate-embedder":
            raise RuntimeError("candidate embedder down")
        return _vec(q)

    @dg.asset(name="questions")
    def questions():
        return pd.DataFrame({"query": [QUERY]})

    with patch.object(mod, "_embed_query", embed), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        res = dg.materialize([questions, comp.build_defs(None).assets[0]], resources={
            "io_manager": dg.InMemoryIOManager(), "context_receipts": dg.ResourceDefinition.hardcoded_resource(store)})
    assert res.success
    r = store.get_receipt(_out(res)["receipt_id"][0])
    assert r["purpose"] is None and _cvs(r) == {"recipe-7"}


def test_candidate_embedding_config_is_used_for_the_candidate(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    seen = []
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1})
    comp.retrievers["cand"]["embedding"] = {"provider": "candidate-embedder"}

    @dg.asset(name="questions")
    def questions():
        return pd.DataFrame({"query": [QUERY]})

    def embed(cfg, q):
        seen.append((cfg or {}).get("provider"))
        return _vec(q)

    with patch.object(mod, "_embed_query", embed), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        dg.materialize([questions, comp.build_defs(None).assets[0]], resources={
            "io_manager": dg.InMemoryIOManager(), "context_receipts": dg.ResourceDefinition.hardcoded_resource(store)})
    assert seen == ["candidate-embedder"]


@pytest.mark.parametrize("shadow, msg", [
    ({"retriever": "cand"}, "fraction"),
    ({"retriever": "cand", "fraction": 0}, "fraction"),
    ({"retriever": "cand", "fraction": 1.5}, "fraction"),
    ({"retriever": "cand", "fraction": True}, "fraction"),
    ({"fraction": 0.1}, "retriever is required"),
    ({"retriever": "ghost", "fraction": 0.1}, "not declared"),
    ({"retriever": "kb", "fraction": 0.1}, "must differ"),
    ({"retriever": "cand", "fraction": 0.1, "replaces": "ghost"}, "not used by any"),
    ({"retriever": "cand", "fraction": 0.1, "nope": 1}, "unknown key"),
    ({"retriever": "cand", "fraction": 0.1, "live_code_version": "a"}, "set together"),
    ({"retriever": "cand", "fraction": 0.1, "live_code_version": "a", "candidate_code_version": "a", "source_asset_key": "k"}, "must differ"),
    ({"retriever": "cand", "fraction": 0.1, "live_code_version": "a", "candidate_code_version": "b"}, "source_asset_key"),
    ({"retriever": "cand", "fraction": 0.1, "seed": 1.5}, "seed"),
])
def test_bad_shadow_blocks_fail_at_build_defs(mod, index, candidate_index, shadow, msg):
    with pytest.raises(ValueError, match=msg):
        _shadow_comp(mod, index, candidate_index, shadow).build_defs(None)


def test_ambiguous_live_retriever_requires_replaces(mod, index, candidate_index):
    steps = [{"id": "a", "op": "retrieve", "retriever": "kb", "top_k": 2},
             {"id": "b", "op": "retrieve", "retriever": "kb2", "top_k": 2},
             {"id": "ans", "op": "generate", "source": "b", "model": "m"}]
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.1}, steps=steps)
    comp.retrievers["kb2"] = dict(comp.retrievers["kb"])
    with pytest.raises(ValueError, match="replaces is required"):
        comp.build_defs(None)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 1, "replaces": "kb2"}, steps=steps)
    comp.retrievers["kb2"] = dict(comp.retrievers["kb"])
    comp.build_defs(None)                                                   # fine once the target is named


def test_no_shadow_block_changes_nothing(mod, index, tmp_path):
    _, store = _store(tmp_path)
    res = _run(mod, _comp(mod, index, {"resource_key": "context_receipts"}), store)
    assert "shadow_served" not in _meta(res)
    assert store.get_receipt(_out(res)["receipt_id"][0])["purpose"] is None
    assert "shadow_vs_live" not in _check_names(_comp(mod, index, {"resource_key": "context_receipts"}))


# --- compare_recipes ---------------------------------------------------------------------------

import random as _random


def _normal(mean, sd, n, seed):
    r = _random.Random(seed)
    return [r.gauss(mean, sd) for _ in range(n)]


def _smp(live, cand):
    return {"L": live, "C": cand}


def test_clearly_better_worse_and_identical(mod):
    live = _normal(0.0, 0.3, 60, 1)
    better = mod.compare_recipes(_smp(live, _normal(0.5, 0.3, 60, 2)), "L", "C")
    assert better["verdict"] == "candidate_better" and better["ci_low"] > 0.05 and better["p_candidate_better"] > 0.99
    worse = mod.compare_recipes(_smp(live, _normal(-0.5, 0.3, 60, 3)), "L", "C")
    assert worse["verdict"] == "live_better" and worse["ci_high"] < -0.05 and worse["p_candidate_better"] < 0.01
    same = mod.compare_recipes(_smp(live, list(live)), "L", "C")
    assert same["verdict"] == "inconclusive" and same["diff"] == 0 and same["ci_low"] < 0 < same["ci_high"]


def test_noisy_gap_above_margin_is_not_called(mod):
    # diff exceeds the margin but the data are far too noisy for n=12: the old threshold would have said candidate_better
    live = _normal(0.0, 1.0, 12, 4)
    cand = [x + 0.0 for x in _normal(0.0, 1.0, 12, 5)]
    r = mod.compare_recipes(_smp(live, cand), "L", "C", min_outcomes=10, margin=0.05)
    assert abs(r["diff"]) > 0.05 and r["verdict"] == "inconclusive"


def test_significant_but_smaller_than_margin_is_inconclusive(mod):
    live = [0.0, 0.01] * 40
    cand = [0.1, 0.11] * 40                      # +0.1 with no noise: CI excludes zero
    assert mod.compare_recipes(_smp(live, cand), "L", "C", margin=0.05)["verdict"] == "candidate_better"
    r = mod.compare_recipes(_smp(live, cand), "L", "C", margin=0.3)
    assert r["ci_low"] > 0 and r["verdict"] == "inconclusive"       # the margin is honoured
    r = mod.compare_recipes(_smp(cand, live), "L", "C", margin=0.3)
    assert r["ci_high"] < 0 and r["verdict"] == "inconclusive"


def test_tiny_sample_missing_side_and_missing_samples_are_insufficient(mod):
    assert mod.compare_recipes(_smp([0.0] * 9, [1.0] * 20), "L", "C")["verdict"] == "insufficient_evidence"
    assert mod.compare_recipes(_smp([0.0] * 20, [1.0] * 9), "L", "C")["verdict"] == "insufficient_evidence"
    assert mod.compare_recipes(_smp([0.0] * 20, []), "L", "C")["verdict"] == "insufficient_evidence"
    assert mod.compare_recipes({"L": [0.0] * 20}, "L", "C")["verdict"] == "insufficient_evidence"
    assert mod.compare_recipes({}, "L", "C")["verdict"] == "insufficient_evidence"
    assert mod.compare_recipes(None, "L", "C")["verdict"] == "insufficient_evidence"
    r = mod.compare_recipes(_smp([0.0] * 9, [1.0] * 9), "L", "C", min_outcomes=9)
    assert r["verdict"] == "candidate_better" and r["n_live"] == r["n_candidate"] == 9


def test_zero_variance(mod):
    r = mod.compare_recipes(_smp([0.2] * 20, [0.5] * 20), "L", "C")
    assert (r["verdict"], r["ci_low"], r["ci_high"], r["diff"]) == ("candidate_better", 0.3, 0.3, 0.3)
    r = mod.compare_recipes(_smp([0.2] * 20, [0.2] * 20), "L", "C", margin=0.0)
    assert r["verdict"] == "inconclusive" and r["ci_low"] == r["ci_high"] == 0
    r = mod.compare_recipes(_smp([0.5] * 20, [0.2] * 20), "L", "C")
    assert r["verdict"] == "live_better"


def test_margin_boundary_is_strict(mod):
    r = mod.compare_recipes(_smp([0.2] * 20, [0.25] * 20), "L", "C", margin=0.05)
    assert r["diff"] == 0.05 and r["verdict"] == "inconclusive"


def test_deterministic_and_seed_sensitive(mod):
    s = _smp(_normal(0, 1, 30, 7), _normal(0.3, 1, 30, 8))
    a = mod.compare_recipes(s, "L", "C", seed=11)
    assert a == mod.compare_recipes(s, "L", "C", seed=11)
    b = mod.compare_recipes(s, "L", "C", seed=12)
    assert (a["ci_low"], a["ci_high"]) != (b["ci_low"], b["ci_high"])
    assert mod.compare_recipes(s, "L", "C", seed="x") == mod.compare_recipes(s, "L", "C", seed="x")


def test_result_reports_everything(mod):
    r = mod.compare_recipes(_smp(_normal(0, 1, 30, 1), _normal(1, 1, 25, 2)), "L", "C", confidence=0.9, resamples=500)
    assert set(r) == {"verdict", "live_mean", "candidate_mean", "n_live", "n_candidate", "diff", "ci_low", "ci_high",
                      "p_candidate_better", "confidence", "resamples", "ready_to_promote", "subsampled", "max_samples",
                      "n_live_used", "n_candidate_used"}
    assert (r["n_live"], r["n_candidate"], r["confidence"], r["resamples"], r["ready_to_promote"]) == (30, 25, 0.9, 500, None)
    assert r["ci_low"] < r["diff"] < r["ci_high"]


def test_confidence_widens_the_interval(mod):
    s = _smp(_normal(0, 1, 40, 1), _normal(0.2, 1, 40, 2))
    n = mod.compare_recipes(s, "L", "C", confidence=0.80)
    w = mod.compare_recipes(s, "L", "C", confidence=0.99)
    assert w["ci_low"] < n["ci_low"] and w["ci_high"] > n["ci_high"]


def test_false_positive_rate_with_no_true_difference(mod):
    # same true mean on both sides: a "candidate_better" verdict is a false positive; one-sided at 95% two-sided CI ~2.5%
    rng = _random.Random(2024)
    runs, fp = 300, 0
    for i in range(runs):
        live = [rng.gauss(0, 1) for _ in range(30)]
        cand = [rng.gauss(0, 1) for _ in range(30)]
        v = mod.compare_recipes(_smp(live, cand), "L", "C", margin=0.0, resamples=400, seed=i)["verdict"]
        fp += v in ("candidate_better", "live_better")
    assert fp / runs <= (1 - 0.95) + 0.04, fp / runs


def test_power_with_a_real_difference(mod):
    rng = _random.Random(99)
    hits = sum(mod.compare_recipes(_smp([rng.gauss(0, 1) for _ in range(40)], [rng.gauss(1, 1) for _ in range(40)]),
                                   "L", "C", resamples=400, seed=i)["verdict"] == "candidate_better" for i in range(60))
    assert hits >= 55


def test_promote_when_is_only_a_reported_boolean(mod):
    s = _smp(_normal(0, 0.3, 40, 1), _normal(0.6, 0.3, 40, 2))
    assert mod.compare_recipes(s, "L", "C")["ready_to_promote"] is None
    assert mod.compare_recipes(s, "L", "C", promote_when={"confidence": 0.99, "min_outcomes": 30})["ready_to_promote"] is True
    assert mod.compare_recipes(s, "L", "C", promote_when={"confidence": 0.99, "min_outcomes": 50})["ready_to_promote"] is False
    close = _smp(_normal(0, 1, 40, 1), _normal(0.35, 1, 40, 2))
    r = mod.compare_recipes(close, "L", "C", confidence=0.80, promote_when={"confidence": 0.9999, "min_outcomes": 10})
    assert r["ready_to_promote"] is False
    r = mod.compare_recipes(_smp([0.0] * 5, [1.0] * 5), "L", "C", min_outcomes=3, promote_when={"min_outcomes": 30})
    assert r["verdict"] == "candidate_better" and r["ready_to_promote"] is False


def test_the_rule_needs_both_the_interval_and_the_margin(mod, monkeypatch):
    """Mutation guards: bounds and margin each flip a verdict, so swapping the bound or dropping the margin is caught."""
    s = _smp(_normal(0, 0.3, 120, 1), _normal(0.2, 0.3, 120, 2))
    base = mod.compare_recipes(s, "L", "C", margin=0.05)
    assert base["verdict"] == "candidate_better" and 0 < base["ci_low"] < base["diff"] < base["ci_high"]
    assert mod.compare_recipes(s, "L", "C", margin=base["diff"] + 0.01)["verdict"] == "inconclusive"       # margin matters
    # interval matters: a gap above the margin whose interval spans zero is not a win, whichever side it is on
    noisy = _smp(_normal(0, 2, 12, 3), _normal(0.5, 2, 12, 4))
    r = mod.compare_recipes(noisy, "L", "C", margin=0.0)
    assert r["ci_low"] < 0 < r["ci_high"] and r["verdict"] == "inconclusive"
    # a negative gap uses the UPPER bound: ci_high < 0 is required for live_better
    r2 = mod.compare_recipes(_smp(noisy["C"], noisy["L"]), "L", "C", margin=0.0)
    assert r2["ci_high"] > 0 and r2["verdict"] == "inconclusive"


@pytest.mark.parametrize("key, bad", [("resamples", 10), ("resamples", 2.5), ("confidence", 1.0), ("confidence", 0),
                                      ("bootstrap_seed", 1.5), ("promote_when", {"confidence": 2}),
                                      ("promote_when", {"nope": 1}), ("promote_when", {"min_outcomes": 0}),
                                      ("max_samples", 5), ("max_samples", 1000.5)])
def test_bootstrap_fields_are_validated(mod, index, candidate_index, key, bad):
    sh = {"retriever": "cand", "fraction": 0.5, **CV, key: bad}
    with pytest.raises(ValueError):
        _shadow_comp(mod, index, candidate_index, sh).build_defs(None)


def test_promote_when_needs_the_check_fields(mod, index, candidate_index):
    with pytest.raises(ValueError, match="promote_when"):
        _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "promote_when": {"confidence": 0.9}}).build_defs(None)


# --- the shadow_vs_live check ------------------------------------------------------------------

def _check_job(mod, comp, store):
    @dg.asset(name="questions")
    def questions():
        return pd.DataFrame({"query": [QUERY]})
    defs = comp.build_defs(None)
    d = dg.Definitions(assets=[questions, *defs.assets], asset_checks=list(defs.asset_checks or []),
                       resources={"io_manager": dg.InMemoryIOManager(),
                                  "context_receipts": dg.ResourceDefinition.hardcoded_resource(store)})
    return d


def _eval(mod, comp, store):
    d = _check_job(mod, comp, store)
    with patch.object(mod, "_embed_query", lambda c, q: _vec(q)), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        res = d.get_implicit_global_asset_job_def().execute_in_process()
    assert res.success
    return [e for e in res.get_asset_check_evaluations() if e.check_name == "shadow_vs_live"]


CV = {"live_code_version": "recipe-7", "candidate_code_version": "recipe-8", "source_asset_key": "support_kb",
      "min_outcomes": 5, "margin": 0.1}


def _check_names(comp):
    return {k.name for c in comp.build_defs(None).asset_checks for k in c.check_keys}


def test_check_is_absent_without_code_versions(mod, index, candidate_index):
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5})
    assert "shadow_vs_live" not in _check_names(comp)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, **CV})
    assert "shadow_vs_live" in _check_names(comp)


def _seed(store, mod, index, candidate_index, live_value, cand_value, n=12):
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5})
    out = _out(_run(mod, comp, store, queries=_queries(n * 4)))
    for r, p in zip((store.get_receipt(i) for i in out["receipt_id"]), _by_purpose(store, out)[1]):
        store.record_outcome(r["receipt_id"], signal="x", value=cand_value if p else live_value, outcome_ref="u")


@pytest.mark.parametrize("live_v, cand_v, passed, verdict", [
    (0.0, 0.8, True, "candidate_better"),
    (0.8, 0.0, False, "live_better"),
    (0.5, 0.5, True, "inconclusive"),
])
def test_check_reads_real_outcomes_and_warns_only_when_live_is_better(mod, index, candidate_index, tmp_path, live_v, cand_v, passed, verdict):
    _, store = _store(tmp_path)
    _seed(store, mod, index, candidate_index, live_v, cand_v)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV})
    (ev,) = _eval(mod, comp, store)
    assert ev.passed is passed
    assert ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["verdict"].value == verdict
    assert ev.metadata["n_live"].value >= 5 and ev.metadata["n_candidate"].value >= 5
    for k in ("diff", "ci_low", "ci_high", "p_candidate_better", "confidence", "resamples"):
        assert k in ev.metadata
    assert "ready_to_promote" in ev.metadata and ev.metadata["ready_to_promote"].value == "n/a"      # no promote_when -> not asserted


def test_check_passes_with_insufficient_evidence(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV})
    (ev,) = _eval(mod, comp, store)                       # the run itself records one receipt, no outcomes yet
    assert ev.passed and ev.metadata["verdict"].value == "insufficient_evidence"


def test_check_reports_ready_to_promote_but_takes_no_action(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    _seed(store, mod, index, candidate_index, 0.0, 0.8)
    sh = {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV, "promote_when": {"confidence": 0.99, "min_outcomes": 5}}
    (ev,) = _eval(mod, _shadow_comp(mod, index, candidate_index, sh), store)
    assert ev.metadata["verdict"].value == "candidate_better" and ev.metadata["ready_to_promote"].value is True
    sh["promote_when"]["min_outcomes"] = 500
    (ev,) = _eval(mod, _shadow_comp(mod, index, candidate_index, sh), store)
    assert ev.metadata["verdict"].value == "candidate_better" and ev.metadata["ready_to_promote"].value is False


def test_check_without_the_new_store_method_degrades_with_a_clear_reason(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)

    class Old:
        def __getattr__(self, n):
            if n == "outcome_samples_by_code_version":
                raise AttributeError(n)
            return getattr(store, n)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV})
    (ev,) = _eval(mod, comp, Old())
    assert ev.passed and ev.metadata["verdict"].value == "insufficient_evidence"
    assert "outcome_samples_by_code_version" in ev.metadata["reason"].value


def test_check_is_reproducible_run_to_run(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    _seed(store, mod, index, candidate_index, 0.1, 0.4)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV})
    (a,) = _eval(mod, comp, store)
    (b,) = _eval(mod, comp, store)
    assert a.metadata["ci_low"].value == b.metadata["ci_low"].value and a.metadata["ci_high"].value == b.metadata["ci_high"].value


def test_large_samples_are_subsampled_deterministically_and_bounded(mod):
    import time
    live, cand = _normal(0.0, 0.5, 200_000, 11), _normal(0.1, 0.5, 200_000, 12)
    t0 = time.time()
    r = mod.compare_recipes(_smp(live, cand), "L", "C", resamples=300, seed=5, max_samples=5000)
    assert time.time() - t0 < 8, "bootstrap cost must be bounded by max_samples, not by the number of receipts"
    assert r["subsampled"] is True and (r["n_live"], r["n_candidate"]) == (200_000, 200_000) and (r["n_live_used"], r["n_candidate_used"]) == (5000, 5000)
    assert abs(r["diff"] - (sum(cand) / len(cand) - sum(live) / len(live))) < 1e-9          # point estimate uses every outcome
    assert r["ci_low"] < 0.1 < r["ci_high"]
    again = mod.compare_recipes(_smp(live, cand), "L", "C", resamples=300, seed=5, max_samples=5000)
    assert (again["ci_low"], again["ci_high"]) == (r["ci_low"], r["ci_high"])                # seeded: stable
    other = mod.compare_recipes(_smp(live, cand), "L", "C", resamples=300, seed=6, max_samples=5000)
    assert (other["ci_low"], other["ci_high"]) != (r["ci_low"], r["ci_high"])


def test_small_samples_are_not_subsampled(mod):
    r = mod.compare_recipes(_smp(_normal(0, 1, 40, 1), _normal(1, 1, 40, 2)), "L", "C", max_samples=100)
    assert r["subsampled"] is False and (r["n_live_used"], r["n_candidate_used"]) == (40, 40)


def test_subsample_is_without_replacement_and_seeded(mod):
    vals = list(range(1000))
    a = mod._subsample(vals, 100, 1, "live")
    assert len(a) == len(set(a)) == 100 and a == mod._subsample(vals, 100, 1, "live") and a != mod._subsample(vals, 100, 2, "live")
