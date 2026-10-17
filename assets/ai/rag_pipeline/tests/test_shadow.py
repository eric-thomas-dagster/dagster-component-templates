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

def _bcv(live=(20, 0.2), cand=(20, 0.5)):
    out = {}
    for k, v in (("L", live), ("C", cand)):
        if v is not None:
            out[k] = {"receipts": v[0] + 5, "outcomes": v[0], "mean_outcome": v[1]}
    return out


@pytest.mark.parametrize("by, kw, verdict", [
    (_bcv(), {}, "candidate_better"),
    (_bcv(cand=(20, -0.2)), {}, "live_better"),
    (_bcv(cand=(20, 0.22)), {}, "inconclusive"),
    (_bcv(cand=(20, 0.25)), {}, "inconclusive"),                  # exactly the margin is not "more than" the margin
    (_bcv(cand=(20, 0.2501)), {}, "candidate_better"),
    (_bcv(cand=(20, 0.15)), {}, "inconclusive"),
    (_bcv(cand=(20, 0.1499)), {}, "live_better"),
    (_bcv(cand=(9, 0.9)), {}, "insufficient_evidence"),            # candidate under min_outcomes
    (_bcv(live=(9, 0.9)), {}, "insufficient_evidence"),            # live under min_outcomes
    (_bcv(cand=None), {}, "insufficient_evidence"),                # candidate has no receipts at all
    (_bcv(live=None), {}, "insufficient_evidence"),
    ({}, {}, "insufficient_evidence"),
    (_bcv(live=(9, 0.2), cand=(9, 0.9)), {"min_outcomes": 9}, "candidate_better"),
    (_bcv(cand=(20, 0.3)), {"margin": 0.2}, "inconclusive"),
    (_bcv(cand=(20, 0.5)), {"margin": 0.2}, "candidate_better"),
])
def test_compare_recipes_verdicts(mod, by, kw, verdict):
    assert mod.compare_recipes(by, "L", "C", **kw)["verdict"] == verdict


def test_compare_recipes_reports_means_and_counts_and_ignores_unanswered_receipts(mod):
    by = {"L": {"receipts": 500, "outcomes": 12, "mean_outcome": 0.1}, "C": {"receipts": 40, "outcomes": 11, "mean_outcome": None}}
    r = mod.compare_recipes(by, "L", "C")
    assert r == {"verdict": "insufficient_evidence", "live_mean": 0.1, "candidate_mean": None, "n_live": 12, "n_candidate": 11}
    by["C"] = {"receipts": 500, "outcomes": 10, "mean_outcome": 0.4}
    r = mod.compare_recipes(by, "L", "C")
    assert (r["verdict"], r["live_mean"], r["candidate_mean"], r["n_live"], r["n_candidate"]) == ("candidate_better", 0.1, 0.4, 12, 10)


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


def test_check_passes_with_insufficient_evidence(mod, index, candidate_index, tmp_path):
    _, store = _store(tmp_path)
    comp = _shadow_comp(mod, index, candidate_index, {"retriever": "cand", "fraction": 0.5, "seed": 5, **CV})
    (ev,) = _eval(mod, comp, store)                       # the run itself records one receipt, no outcomes yet
    assert ev.passed and ev.metadata["verdict"].value == "insufficient_evidence"
