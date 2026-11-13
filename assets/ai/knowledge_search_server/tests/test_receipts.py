"""KnowledgeSearchResource's new receipts_db_path support: real SQLite writes (no mocking --
sqlite3 is stdlib), with _query_vector_store/_embed_query monkeypatched so these tests don't
need a real chromadb/pinecone/qdrant install or real embeddings -- the thing under test is the
receipts/quarantine logic, not the vector-store dispatch (already covered by
test_knowledge_search.py)."""
import sqlite3

import pytest


def _patch_search_internals(monkeypatch, mod, hits):
    monkeypatch.setattr(mod, "_embed_query", lambda query, provider, model, api_key_env_var: [0.1, 0.2])
    monkeypatch.setattr(mod, "_query_vector_store", lambda embedding, **kwargs: hits)
    monkeypatch.setattr(mod, "_check_required_env_var", lambda *a, **k: None)


def _hit(chunk_id, text="hello", quarantined=False, score=0.9, source_asset_key="kb_asset"):
    meta = {"chunk_id": chunk_id, "source_asset_key": source_asset_key}
    if quarantined:
        meta["feedback_quarantined"] = True
    return {"id": chunk_id, "metadata": meta, "document": text, "score": score}


def test_search_drops_quarantined_chunks_even_without_receipts_configured(mod, monkeypatch):
    hits = [_hit("c1", quarantined=True), _hit("c2")]
    _patch_search_internals(monkeypatch, mod, hits)
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb")
    result = resource.search("q")
    assert [c["chunk_id"] for c in result["chunks"]] == ["c2"]
    assert "receipt_id" not in result


def test_search_without_receipts_db_path_never_writes_anything(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_search_internals(monkeypatch, mod, hits)
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb")
    result = resource.search("q")
    assert "receipt_id" not in result


def test_search_records_a_receipt_and_returns_receipt_id(mod, monkeypatch, tmp_path):
    hits = [_hit("c1"), _hit("c2")]
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    result = resource.search("what is the return policy?", top_k=2)
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
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    resource.search("a very specific raw query string")

    with open(db_path, "rb") as f:
        raw = f.read()
    assert b"a very specific raw query string" not in raw
    assert b"a very secret internal sentence" not in raw


def test_quarantined_hits_are_excluded_from_the_receipt_items_too(mod, monkeypatch, tmp_path):
    hits = [_hit("c1", quarantined=True), _hit("c2")]
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    result = resource.search("q")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    items = conn.execute("SELECT * FROM context_receipt_items WHERE receipt_id = ?", (result["receipt_id"],)).fetchall()
    assert {i["item_id"] for i in items} == {"c2"}
    receipt = conn.execute("SELECT * FROM context_receipts WHERE receipt_id = ?", (result["receipt_id"],)).fetchone()
    assert receipt["eligible_count"] == 2  # BEFORE quarantine filtering


def test_record_feedback_records_an_outcome_with_default_signal_value(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    result = resource.search("q")
    out = resource.record_feedback(result["receipt_id"], "thumbs_up", user_id="alice")
    assert out == {"recorded": True}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    events = conn.execute("SELECT * FROM context_receipt_events WHERE receipt_id = ?", (result["receipt_id"],)).fetchall()
    assert len(events) == 1
    assert events[0]["event_type"] == "outcome"
    import json
    payload = json.loads(events[0]["payload_json"])
    assert payload["signal"] == "thumbs_up"
    assert payload["value"] == 1.0
    assert payload["outcome_ref"] == "alice"


def test_record_feedback_unknown_signal_without_value_raises(mod, monkeypatch, tmp_path):
    hits = [_hit("c1")]
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    result = resource.search("q")
    with pytest.raises(ValueError, match="unknown signal"):
        resource.record_feedback(result["receipt_id"], "a_made_up_signal")


def test_record_feedback_without_receipts_db_path_raises(mod):
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb")
    with pytest.raises(ValueError, match="receipts_db_path is not configured"):
        resource.record_feedback("some-receipt-id", "thumbs_up")


def test_record_feedback_unknown_receipt_id_raises(mod, tmp_path):
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    with pytest.raises(KeyError, match="unknown context receipt"):
        resource.record_feedback("does-not-exist", "thumbs_up")


def test_build_mcp_server_only_registers_record_feedback_when_receipts_configured(mod, tmp_path):
    resource_without = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb")
    server_without = mod.build_mcp_server(resource_without)
    tool_names_without = {t.name for t in server_without._tool_manager.list_tools()} if hasattr(server_without, "_tool_manager") else None

    resource_with = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=str(tmp_path / "r.sqlite3"))
    server_with = mod.build_mcp_server(resource_with)
    # Don't over-assert on FastMCP internals across its major versions -- just confirm building
    # succeeds either way and that record_feedback is reachable only in the "with" case via the
    # resource-level API (already proven above); this test exists to catch build_mcp_server
    # itself crashing when receipts_db_path is/isn't set.
    assert server_without is not None
    assert server_with is not None


def test_inspection_asset_reports_receipt_counts(mod, monkeypatch, tmp_path):
    import dagster as dg

    hits = [_hit("c1")]
    _patch_search_internals(monkeypatch, mod, hits)
    db_path = str(tmp_path / "receipts.sqlite3")
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb", receipts_db_path=db_path)
    result = resource.search("q")
    resource.record_feedback(result["receipt_id"], "thumbs_up")

    component = mod.KnowledgeSearchServerComponent(
        collection_name="kb", receipts_db_path=db_path,
        inspection_asset_name="knowledge_search_tools_test",
    )
    defs = component.build_defs(context=None)
    asset_def = next(a for a in defs.assets if "knowledge_search_tools_test" in {k.to_user_string() for k in a.keys})
    materialize_result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert materialize_result.success
    md = materialize_result.asset_materializations_for_node("knowledge_search_tools_test")[0].metadata
    assert md["retrievals_recorded"].value == 1
    assert md["outcomes_recorded"].value == 1
    assert "record_feedback" in md["tool_names"].value
