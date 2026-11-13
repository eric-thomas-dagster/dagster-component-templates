"""HybridKnowledgeSearchResource's new receipts_db_path support: real SQLite writes (no
mocking -- sqlite3 is stdlib), with _query_vector_store/_embed_query monkeypatched so these
tests don't need a real chromadb/embeddings install or a real graph file -- the thing under
test is the receipts/quarantine logic on the vector side of search(), not vector-store dispatch
or graph traversal (already covered by test_hybrid_knowledge_search_server.py)."""
import sqlite3

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _patch_vector_internals(monkeypatch, mod, hits):
    monkeypatch.setattr(mod, "_embed_query", lambda query, provider, model, api_key_env_var: [0.1, 0.2])
    monkeypatch.setattr(mod, "_query_vector_store", lambda embedding, **kwargs: hits)


def _hit(chunk_id, text="hello", quarantined=False, score=0.9, source_asset_key="kb_asset"):
    meta = {"chunk_id": chunk_id, "source_asset_key": source_asset_key}
    if quarantined:
        meta["feedback_quarantined"] = True
    return {"id": chunk_id, "metadata": meta, "document": text, "score": score}


def _resource(mod, **overrides):
    kwargs = {"vector_store": {"provider": "chromadb", "collection_name": "kb"}, "graph": {"provider": "networkx"}}
    kwargs.update(overrides)
    return mod.HybridKnowledgeSearchResource(**kwargs)


def test_search_mode_vector_drops_quarantined_chunks(mod, monkeypatch):
    hits = [_hit("c1", quarantined=True), _hit("c2")]
    _patch_vector_internals(monkeypatch, mod, hits)
    resource = _resource(mod)
    result = resource.search("q", mode="vector")
    assert [p["chunk_id"] for p in result["passages"]] == ["c2"]
    assert "receipt_id" not in result


def test_search_mode_graph_never_records_a_receipt_even_when_configured(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    # mode='graph' never calls _vector_search at all, so no vector_hits -> no receipt,
    # regardless of receipts_db_path. (graph.provider='networkx' with no real graph file just
    # returns nothing found, which is fine -- this test only cares that no receipt_id appears.)
    result = resource.search("q", mode="graph")
    assert "receipt_id" not in result


def test_search_mode_vector_records_a_receipt_and_returns_receipt_id(mod, monkeypatch, tmp_path):
    hits = [_hit("c1"), _hit("c2")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    result = resource.search("what is the return policy?", top_k=2, mode="vector")
    assert "receipt_id" in result and result["receipt_id"]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    receipt = conn.execute("SELECT * FROM context_receipts WHERE receipt_id = ?", (result["receipt_id"],)).fetchone()
    assert receipt is not None
    assert receipt["context_name"] == "kb"
    assert receipt["eligible_count"] == 2
    items = conn.execute("SELECT * FROM context_receipt_items WHERE receipt_id = ?", (result["receipt_id"],)).fetchall()
    assert {i["item_id"] for i in items} == {"c1", "c2"}


def test_receipt_never_contains_the_raw_query_or_chunk_text(mod, monkeypatch, tmp_path):
    hits = [_hit("c1", text="a very secret internal sentence")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    resource.search("a very specific raw query string", mode="vector")

    with open(db_path, "rb") as f:
        raw = f.read()
    assert b"a very specific raw query string" not in raw
    assert b"a very secret internal sentence" not in raw


def test_quarantined_hits_excluded_from_receipt_items_too(mod, monkeypatch, tmp_path):
    hits = [_hit("c1", quarantined=True), _hit("c2")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    result = resource.search("q", mode="vector")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    items = conn.execute("SELECT * FROM context_receipt_items WHERE receipt_id = ?", (result["receipt_id"],)).fetchall()
    assert {i["item_id"] for i in items} == {"c2"}
    receipt = conn.execute("SELECT * FROM context_receipts WHERE receipt_id = ?", (result["receipt_id"],)).fetchone()
    assert receipt["eligible_count"] == 2  # BEFORE quarantine filtering


def test_record_feedback_records_an_outcome(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    result = resource.search("q", mode="vector")
    out = resource.record_feedback(result["receipt_id"], "thumbs_up", user_id="alice")
    assert out == {"recorded": True}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    events = conn.execute("SELECT * FROM context_receipt_events WHERE receipt_id = ?", (result["receipt_id"],)).fetchall()
    assert len(events) == 1
    assert events[0]["event_type"] == "outcome"


def test_record_feedback_without_receipts_db_path_raises(mod):
    resource = _resource(mod)
    with pytest.raises(ValueError, match="receipts_db_path is not configured"):
        resource.record_feedback("some-id", "thumbs_up")


def test_record_feedback_unknown_receipt_id_raises(mod, tmp_path):
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    with pytest.raises(KeyError, match="unknown context receipt"):
        resource.record_feedback("does-not-exist", "thumbs_up")


def test_build_mcp_server_builds_with_and_without_receipts(mod, tmp_path):
    server_without = mod.build_mcp_server(_resource(mod))
    server_with = mod.build_mcp_server(_resource(mod, receipts_db_path=str(tmp_path / "r.sqlite3")))
    assert server_without is not None
    assert server_with is not None


def test_inspection_asset_reports_receipt_counts(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_vector_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = _resource(mod, receipts_db_path=db_path)
    result = resource.search("q", mode="vector")
    resource.record_feedback(result["receipt_id"], "thumbs_up")

    # check_overlap() (called unconditionally by the inspection asset) needs a vector/graph
    # provider combination it knows how to introspect cheaply -- chromadb+networkx with no
    # real files just returns overlap_fraction=None via its own except-handled path, which is
    # fine; this test only cares about the receipt counts, not the overlap numbers.
    monkeypatch.setattr(mod.HybridKnowledgeSearchResource, "check_overlap", lambda self: {
        "overlap_fraction": None, "vector_chunk_count": None, "graph_chunk_count": None, "overlap_note": "n/a",
    })

    import dagster as dg
    component = mod.HybridKnowledgeSearchServerComponent(
        vector_store={"provider": "chromadb", "collection_name": "kb"},
        graph={"provider": "networkx"},
        receipts_db_path=db_path,
        inspection_asset_name="hybrid_knowledge_search_tools_test",
    )
    defs = component.build_defs(context=None)
    asset_def = next(a for a in defs.assets if "hybrid_knowledge_search_tools_test" in {k.to_user_string() for k in a.keys})
    materialize_result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert materialize_result.success
    md = materialize_result.asset_materializations_for_node("hybrid_knowledge_search_tools_test")[0].metadata
    assert md["retrievals_recorded"].value == 1
    assert md["outcomes_recorded"].value == 1
    assert "record_feedback" in md["tool_names"].value
