"""The whole loop, with real components: build the knowledge base -> serve queries through rag_pipeline (which records its own
receipts) -> people rate the answers -> rebuild: the build reads those receipts and stops serving what keeps failing.

Real chromadb, real ContextReceiptStore, real context_engineering_pipeline and rag_pipeline code; only the embedding and the LLM
network calls are faked."""
import hashlib
import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import requires_chromadb

pytestmark = requires_chromadb
ROOT = Path(__file__).resolve().parents[4]
KEY = "context_receipts"
DOCS = [
    ("billing_a", "Duplicate charges on your subscription are refunded automatically within five business days."),
    ("billing_b", "Invoices are emailed on the first day of each month and can be downloaded from the billing page."),
    ("billing_c", "A charged twice problem usually means a second workspace was created by accident, close it for a refund."),
    ("export_a", "To export a report open it, choose Export, then pick CSV or PDF and wait for the email."),
    ("ship_a", "Standard shipping takes five to seven business days and express shipping takes two."),
    ("ship_b", "Tracking numbers are emailed as soon as the parcel leaves the warehouse."),
]
QUERY = "Why was I charged twice for my subscription?"


def vec(text):
    v = [0.0] * 64
    for w in re.findall(r"[a-z]+", text.lower()):
        v[int(hashlib.md5(w.encode()).hexdigest(), 16) % 64] += 1.0
    n = sum(x * x for x in v) ** 0.5 or 1.0
    return [x / n for x in v]


def fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": vec(t)} for t in input]
    return resp


def fake_completion(**kw):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content="An answer.", tool_calls=None))]
    resp.usage = MagicMock(total_tokens=3, model_dump=lambda: {"total_tokens": 3})
    return resp


def load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_receipts_from_real_queries_change_the_next_build_and_what_gets_served(tmp_path):
    cep = load("assets/ai/context_engineering_pipeline/component.py", "cep_loop")
    rag = load("assets/ai/rag_pipeline/component.py", "rag_loop")
    store = load("resources/context_receipt_store/component.py", "store_loop").ContextReceiptStore(str(tmp_path / "r.sqlite3"))
    kb = str(tmp_path / "kb")

    def build(**receipts):
        comp = cep.ContextEngineeringPipelineComponent(
            asset_name="support_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="body", code_version="recipe-7",
            receipts={"resource_key": KEY, "quarantine": {"mode": "exclude"}, **receipts},
            steps=[{"id": "chunks", "op": "chunk", "strategy": "fixed", "chunk_size": 500, "chunk_overlap": 0, "min_chunk_chars": 10},
                   {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-embed"},
                   {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "connection_string": kb, "collection_name": "support_kb"}])
        frame = pd.DataFrame(DOCS, columns=["doc_id", "body"])

        @dg.asset(name="raw_docs")
        def raw_docs():
            return frame

        with patch("litellm.embedding", side_effect=fake_embedding):
            res = dg.materialize([raw_docs, *comp.build_defs(None).assets], resources={KEY: store}, instance=dg.DagsterInstance.ephemeral())
        assert res.success
        return res

    def indexed():
        import chromadb
        return set(chromadb.PersistentClient(path=kb).get_collection("support_kb").get()["ids"])

    def ask():
        comp = rag.RAGPipelineComponent(
            asset_name="answers", upstream_asset_key="questions", receipts={"resource_key": KEY, "context_name": "support_kb"},
            retrievers={"kb": {"kind": "chromadb", "path": kb, "collection_name": "support_kb"}},
            steps=[{"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 2},
                   {"id": "ans", "op": "generate", "source": "found", "model": "gpt-4o-mini"}])

        @dg.asset(name="questions")
        def questions():
            return pd.DataFrame({"query": [QUERY]})

        with patch.object(rag, "_embed_query", lambda cfg, q: vec(q)), patch("litellm.completion", side_effect=fake_completion), \
                patch("litellm.completion_cost", return_value=0.0):
            res = dg.materialize([questions, comp.build_defs(None).assets[0]],
                                 resources={"io_manager": dg.InMemoryIOManager(), KEY: dg.ResourceDefinition.hardcoded_resource(store)})
        assert res.success
        rid = res.output_for_node("answers")["receipt_id"][0]
        return rid, [i["item_id"] for i in store.get_receipt(rid)["items"]]       # what was actually served, per the receipt

    # 1) first build: nothing is known yet, everything is indexed and stamped
    build()
    assert indexed() == {f"{d}_0" for d, _ in DOCS}

    # 2) real queries through rag_pipeline: it records the receipts itself (provenance from the pipeline's stamps)
    first_served = None
    for _ in range(6):
        rid, served = ask()
        first_served = first_served or served
        assert store.get_receipt(rid)["items"][0]["source_code_version"] == "recipe-7"
        store.record_outcome(rid, signal="thumbs_down")                    # the answers built from these chunks keep disappointing people
    assert len(first_served) == 2

    # 3) the next build reads those receipts and stops serving what keeps failing
    res = build()
    after = indexed()
    assert not (set(first_served) & after), "the chunks users kept rating badly are out of the index"
    assert after == {f"{d}_0" for d, _ in DOCS} - set(first_served)           # and nothing else was touched
    md = next(e.materialization.metadata for e in res.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("support_kb"))
    summary = md["receipts_feedback"].value
    assert sorted(summary["quarantined_ids"]) == sorted(first_served) and summary["scoped_to_recipe"] == "recipe-7"

    # 4) the loop is closed: the same question is now answered from different chunks
    _, served_now = ask()
    assert not (set(served_now) & set(first_served))
