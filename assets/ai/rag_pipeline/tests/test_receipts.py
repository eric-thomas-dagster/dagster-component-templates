"""Closed-loop context receipts for rag_pipeline.

Real chromadb, a real ContextReceiptStore (sqlite in tmp_path) registered as a Dagster resource, and the REAL
context_engineering_pipeline component writes the index. Only the embedding / completion network calls are faked."""
import hashlib
import importlib.util
import re
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

chromadb = pytest.importorskip("chromadb")
ROOT = Path(__file__).resolve().parents[4]

DOCS = [
    ("billing_a", "Duplicate charges on your subscription are refunded automatically within five business days."),
    ("billing_b", "Invoices are emailed on the first day of each month and can be downloaded from the billing page."),
    ("billing_c", "A charged twice problem usually means a second workspace was created by accident, close it for a refund."),
    ("export_a", "To export a report open it, choose Export, then pick CSV or PDF and wait for the email."),
    ("export_b", "Scheduled report exports are delivered to your inbox as attachments every day or week."),
    ("ship_a", "Standard shipping takes five to seven business days and express shipping takes two."),
    ("ship_b", "Tracking numbers are emailed as soon as the parcel leaves the warehouse."),
    ("ship_c", "Returns are accepted within thirty days if the parcel is unopened."),
]
QUERY = "Why was I charged twice for my subscription?"


def _vec(text):
    v = [0.0] * 64
    for w in re.findall(r"[a-z]+", text.lower()):
        v[int(hashlib.md5(w.encode()).hexdigest(), 16) % 64] += 1.0
    n = sum(x * x for x in v) ** 0.5 or 1.0
    return [x / n for x in v]


def _fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": _vec(t)} for t in input]
    return resp


def _fake_completion(**kw):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content="Because of a duplicate workspace.", tool_calls=None))]
    resp.usage = MagicMock(total_tokens=3, model_dump=lambda: {"total_tokens": 3})
    return resp


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def index(tmp_path_factory):
    """Build the vector index with the real context_engineering_pipeline component."""
    base = tmp_path_factory.mktemp("kb")
    cep = _load(ROOT / "assets/ai/context_engineering_pipeline/component.py", "cep_for_rag_receipts")
    kb = str(base / "kb")
    comp = cep.ContextEngineeringPipelineComponent(
        asset_name="support_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="body",
        code_version="recipe-7",
        steps=[
            {"id": "chunks", "op": "chunk", "strategy": "fixed", "chunk_size": 500, "chunk_overlap": 0, "min_chunk_chars": 10},
            {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-embed"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "connection_string": kb,
             "collection_name": "support_kb", "sync_mode": "upsert"},
        ],
    )
    frame = pd.DataFrame(DOCS, columns=["doc_id", "body"])

    @dg.asset(name="raw_docs")
    def raw_docs():
        return frame

    with patch("litellm.embedding", side_effect=_fake_embedding):
        res = dg.materialize([raw_docs, *comp.build_defs(None).assets], instance=dg.DagsterInstance.ephemeral())
    assert res.success
    return kb


def _store(tmp_path):
    r = _load(ROOT / "resources/context_receipt_store/component.py", "receipt_store_for_rag_tests")
    return r, r.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def _steps():
    return [
        {"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 2},
        {"id": "ans", "op": "generate", "source": "found", "model": "gpt-4o-mini"},
    ]


def _comp(mod, index, receipts=None, steps=None):
    return mod.RAGPipelineComponent(
        asset_name="answers", upstream_asset_key="questions", steps=steps or _steps(), receipts=receipts,
        retrievers={"kb": {"kind": "chromadb", "path": index, "collection_name": "support_kb"}},
    )


def _run(mod, comp, store=None, resource_key="context_receipts", queries=(QUERY,), fake_resource=None):
    @dg.asset(name="questions")
    def questions():
        return pd.DataFrame({"query": list(queries)})

    resources = {"io_manager": dg.InMemoryIOManager()}
    if store is not None or fake_resource is not None:
        resources[resource_key] = dg.ResourceDefinition.hardcoded_resource(fake_resource or store)
    with patch.object(mod, "_embed_query", lambda cfg, q: _vec(q)), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        res = dg.materialize([questions, comp.build_defs(None).assets[0]], resources=resources)
    assert res.success
    return res


@pytest.fixture()
def mod():
    return load_component_module()


def _meta(res):
    ev = [e for e in res.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("answers")][0]
    return dict(ev.materialization.metadata)


def _out(res):
    return res.output_for_node("answers")


# --- behaviour with receipts omitted ------------------------------------------------------------

def test_receipts_omitted_is_unchanged(mod, index):
    comp = _comp(mod, index)
    out = _out(_run(mod, comp))
    assert "receipt_id" not in out.columns
    assert len(out["sources"][0]) == 2
    assert not any(k in out["sources"][0][0] for k in ("item_id", "selection", "adjusted_score"))
    spec = comp.build_defs(None).assets[0]
    assert spec.required_resource_keys == {"io_manager"}


# --- recording ---------------------------------------------------------------------------------

def test_receipt_carries_the_build_sides_provenance(mod, index, tmp_path):
    _, store = _store(tmp_path)
    comp = _comp(mod, index, {"resource_key": "context_receipts", "caller_id": "support-bot", "purpose": "answer", "policy_version": "p1"})
    assert comp.build_defs(None).assets[0].required_resource_keys >= {"context_receipts"}
    res = _run(mod, comp, store)
    out = _out(res)
    rid = out["receipt_id"][0]
    assert rid
    receipt = store.get_receipt(rid)
    assert receipt["context_name"] == "answers"            # default: asset_name
    assert receipt["caller_id"] == "support-bot" and receipt["purpose"] == "answer" and receipt["policy_version"] == "p1"
    assert receipt["query_hash"] == hashlib.sha256(QUERY.encode()).hexdigest()
    assert receipt["dagster_run_id"] == res.run_id and receipt["dagster_asset_key"] == "answers"
    assert receipt["eligible_count"] == 2 and receipt["excluded_count"] == 0
    items = receipt["items"]
    assert [i["rank"] for i in items] == [1, 2]
    source_chunks = {m["chunk_id"]: m for m in chromadb.PersistentClient(path=index).get_collection("support_kb").get(include=["metadatas"])["metadatas"]}
    for i in items:
        meta = source_chunks[i["item_id"]]
        assert i["source_asset_key"] == "support_kb" == meta["source_asset_key"]
        assert i["source_version"] and i["source_version"] == meta["source_version"]
        assert i["source_code_version"] == "recipe-7"
        assert i["selection"] == "ranked" and i["score"] is not None
    assert receipt["context_version"] == items[0]["source_version"]   # all chunks share one build
    md = _meta(res)
    assert md["receipt_ids"].value == [rid] and md["receipts_recorded"].value == 1


def test_one_receipt_per_row(mod, index, tmp_path):
    _, store = _store(tmp_path)
    comp = _comp(mod, index, {"resource_key": "context_receipts", "context_name": "kb_answers"})
    out = _out(_run(mod, comp, store, queries=[QUERY, "How do I export a report?"]))
    ids = out["receipt_id"].tolist()
    assert len(set(ids)) == 2 and all(store.get_receipt(i)["context_name"] == "kb_answers" for i in ids)


def test_hybrid_search_path_records_too(mod, index, tmp_path, monkeypatch):
    try:
        import rank_bm25  # noqa: F401
    except ImportError:  # keyword scorer stand-in so the hybrid path is exercised without the optional dependency
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
    comp = _comp(mod, index, {"resource_key": "context_receipts", "rerank": {"explore_fraction": 0}}, steps)
    rid = _out(_run(mod, comp, store))["receipt_id"][0]
    assert len(store.get_receipt(rid)["items"]) == 2


# --- learning ----------------------------------------------------------------------------------

def test_a_thumbs_down_makes_that_chunk_rank_lower_next_time(mod, index, tmp_path):
    _, store = _store(tmp_path)
    cfg = {"resource_key": "context_receipts", "rerank": {"prior_weight": 100, "explore_fraction": 0, "fetch_multiplier": 8}}
    comp = _comp(mod, index, cfg, steps=[{"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 3},
                                         {"id": "ans", "op": "generate", "source": "found", "model": "gpt-4o-mini"}])
    first = store.get_receipt(_out(_run(mod, comp, store))["receipt_id"][0])
    assert first["eligible_count"] == len(DOCS)           # over-fetched the whole (small) index
    top = first["items"][0]["item_id"]
    store.record_outcome(first["receipt_id"], signal="thumbs_down", value=-1, outcome_ref="u1")
    # every item in that receipt is blamed; only the top one is in the next query's decision, so blame just it
    assert store.refresh_item_scores() >= 1
    second = store.get_receipt(_out(_run(mod, comp, store))["receipt_id"][0])
    ids = [i["item_id"] for i in second["items"]]
    assert ids[0] != top, "the down-voted chunk must not stay on top"
    assert top not in ids or ids.index(top) > 0


def test_exploration_is_marked_on_the_receipt(mod, index, tmp_path):
    _, store = _store(tmp_path)
    cfg = {"resource_key": "context_receipts", "rerank": {"explore_fraction": 1, "fetch_multiplier": 4}}
    out = _out(_run(mod, _comp(mod, index, cfg), store))
    items = store.get_receipt(out["receipt_id"][0])["items"]
    assert [i["selection"] for i in items] == ["ranked", "explore"]
    assert out["sources"][0][-1]["selection"] == "explore"


def test_record_only_mode_never_reorders(mod, index, tmp_path):
    _, store = _store(tmp_path)
    plain = _out(_run(mod, _comp(mod, index)))["sources"][0]
    recorded = _out(_run(mod, _comp(mod, index, {"resource_key": "context_receipts"}), store))["sources"][0]
    assert [d["text"] for d in plain] == [d["text"] for d in recorded]


# --- failure handling --------------------------------------------------------------------------

class _Broken:
    def record_retrieval(self, **kw):
        raise RuntimeError("receipt db down")

    def get_item_scores(self, ids):
        raise RuntimeError("receipt db down")


def test_store_failure_does_not_break_the_query_by_default(mod, index):
    cfg = {"resource_key": "context_receipts", "rerank": {"explore_fraction": 0}}
    res = _run(mod, _comp(mod, index, cfg), fake_resource=_Broken())
    out = _out(res)
    assert out["receipt_id"][0] is None and out["answer"][0]
    assert len(out["sources"][0]) == 2                       # fell back to plain top_k
    assert _meta(res)["receipts_failed"].value == 1


def test_store_failure_raises_with_fail_on_error(mod, index):
    cfg = {"resource_key": "context_receipts", "fail_on_error": True}
    comp = _comp(mod, index, cfg)

    @dg.asset(name="questions")
    def questions():
        return pd.DataFrame({"query": [QUERY]})

    with patch.object(mod, "_embed_query", lambda c, q: _vec(q)), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        res = dg.materialize([questions, comp.build_defs(None).assets[0]], raise_on_error=False,
                             resources={"io_manager": dg.InMemoryIOManager(),
                                        "context_receipts": dg.ResourceDefinition.hardcoded_resource(_Broken())})
    assert not res.success


# --- validation --------------------------------------------------------------------------------

@pytest.mark.parametrize("bad, msg", [
    ({"resource_key": "r", "nope": 1}, "unknown key"),
    ({}, "resource_key is required"),
    ({"resource_key": "r", "rerank": {"prior_weigth": 1}}, "unknown key"),
    ({"resource_key": "r", "rerank": {"explore_fraction": 2}}, "between 0 and 1"),
    ({"resource_key": "r", "rerank": {"fetch_multiplier": 0}}, "fetch_multiplier"),
    ({"resource_key": "r", "fail_on_error": "yes"}, "fail_on_error"),
])
def test_bad_receipts_blocks_fail_at_build_defs(mod, index, bad, msg):
    with pytest.raises(ValueError, match=msg):
        _comp(mod, index, bad).build_defs(None)
