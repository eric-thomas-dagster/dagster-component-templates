"""KnowledgeSearch: the gateway-side helper a FastMCP (or any) tool uses instead of
hand-written vector search code. Lives in component.py itself (not a separate gateway.py --
this repo's self-containment rule: a component dir holds component.py + __init__.py, nothing
else). The chromadb + sentence_transformers path is exercised for real (both are free/local, no
paid API, no mocking needed) end-to-end: real embeddings, a real chromadb collection, a real
query. pinecone/qdrant are covered with their SDK client classes mocked (the one paid-API
boundary for those two providers) -- skipped when the SDK itself isn't installed, since
`patch("pinecone.Pinecone", ...)` / `patch("qdrant_client.QdrantClient", ...)` still need the
real package importable to resolve the dotted patch target, even though the client itself gets
replaced with a Mock.
"""
import importlib.util
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

HERE = Path(__file__).resolve().parent.parent


def _load(name, rel_path):
    spec = importlib.util.spec_from_file_location(name, HERE / rel_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gateway = _load("context_receipt_store_component", "component.py")
KnowledgeSearch = gateway.KnowledgeSearch


def _chromadb_available():
    try:
        import chromadb  # noqa: F401
        import sentence_transformers  # noqa: F401
        return True
    except ImportError:
        return False


def _pinecone_available():
    try:
        import pinecone  # noqa: F401
        return True
    except ImportError:
        return False


def _qdrant_client_available():
    try:
        import qdrant_client  # noqa: F401
        return True
    except ImportError:
        return False


requires_chromadb = pytest.mark.skipif(not _chromadb_available(), reason="requires chromadb + sentence-transformers")
requires_pinecone = pytest.mark.skipif(not _pinecone_available(), reason="requires pinecone")
requires_qdrant_client = pytest.mark.skipif(not _qdrant_client_available(), reason="requires qdrant-client")


@requires_chromadb
def test_real_chromadb_search_excludes_quarantined_and_ranks_by_relevance(tmp_path):
    import chromadb
    from sentence_transformers import SentenceTransformer

    db_path = str(tmp_path / "chroma")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    texts = [
        "Our return policy is 30 days.",
        "The weather today is sunny and warm.",
        "We sell shoes and clothing online.",
    ]
    embeddings = model.encode(texts, normalize_embeddings=True).tolist()

    client = chromadb.PersistentClient(path=db_path)
    coll = client.get_or_create_collection(name="kb_test")
    coll.upsert(
        ids=["c1", "c2", "c3"],
        embeddings=embeddings,
        documents=texts,
        metadatas=[
            {"chunk_id": "c1", "source_asset_key": "docs", "source_version": "v1", "source_code_version": "cv1", "text": texts[0], "feedback_quarantined": False},
            {"chunk_id": "c2", "source_asset_key": "docs", "source_version": "v1", "source_code_version": "cv1", "text": texts[1], "feedback_quarantined": True},
            {"chunk_id": "c3", "source_asset_key": "docs", "source_version": "v1", "source_code_version": "cv1", "text": texts[2], "feedback_quarantined": False},
        ],
    )

    ks = KnowledgeSearch(provider="chromadb", connection_string=db_path, collection_name="kb_test", embed_provider="sentence_transformers")
    result = ks.search("what is your return policy?", context_name="kb", top_k=3, purpose="test")

    chunk_ids = [c["chunk_id"] for c in result["chunks"]]
    assert "c2" not in chunk_ids  # quarantined, dropped
    assert chunk_ids[0] == "c1"  # the real relevant match ranks first
    assert result["chunks"][0]["text"] == texts[0]

    receipt = result["_receipt"]
    assert receipt["context_name"] == "kb"
    assert receipt["purpose"] == "test"
    assert receipt["eligible_count"] == 3  # pre-quarantine-filter count
    assert len(receipt["items"]) == 2
    assert receipt["items"][0] == {
        "item_id": "c1", "source_asset_key": "docs", "source_version": "v1",
        "source_code_version": "cv1", "rank": 0, "score": None,
    }


@requires_chromadb
def test_receipt_never_contains_query_text_or_chunk_content(tmp_path):
    import chromadb
    from sentence_transformers import SentenceTransformer

    db_path = str(tmp_path / "chroma2")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    text = "a secret internal document about pricing"
    embedding = model.encode([text], normalize_embeddings=True).tolist()

    client = chromadb.PersistentClient(path=db_path)
    coll = client.get_or_create_collection(name="kb_test2")
    coll.upsert(ids=["c1"], embeddings=embedding, documents=[text], metadatas=[{"chunk_id": "c1", "text": text}])

    ks = KnowledgeSearch(provider="chromadb", connection_string=db_path, collection_name="kb_test2", embed_provider="sentence_transformers")
    query = "what is the secret pricing document about?"
    result = ks.search(query, context_name="kb", top_k=1)

    receipt_str = str(result["_receipt"])
    assert query not in receipt_str
    assert text not in receipt_str
    assert result["_receipt"]["query_hash"] != query


@requires_pinecone
def test_pinecone_provider_mocked_sdk():
    fake_match = {"id": "c1", "score": 0.9, "metadata": {"chunk_id": "c1", "source_asset_key": "docs", "source_version": "v1", "source_code_version": "cv1", "text": "hello"}}
    fake_index = MagicMock()
    fake_index.query.return_value = {"matches": [fake_match]}
    fake_pc = MagicMock()
    fake_pc.Index.return_value = fake_index

    with patch("pinecone.Pinecone", return_value=fake_pc), \
         patch.object(gateway, "_embed_query", return_value=[0.1, 0.2, 0.3]):
        ks = KnowledgeSearch(provider="pinecone", collection_name="kb", embed_provider="sentence_transformers")
        result = ks.search("hello", context_name="kb", top_k=1)

    assert result["chunks"][0]["chunk_id"] == "c1"
    assert result["chunks"][0]["text"] == "hello"
    assert result["_receipt"]["items"][0]["score"] == 0.9


@requires_qdrant_client
def test_qdrant_provider_mocked_sdk():
    fake_hit = MagicMock(id="c1", score=0.8, payload={"chunk_id": "c1", "source_asset_key": "docs", "source_version": "v1", "source_code_version": "cv1", "text": "hi"})
    fake_client = MagicMock()
    fake_client.search.return_value = [fake_hit]

    with patch("qdrant_client.QdrantClient", return_value=fake_client), \
         patch.object(gateway, "_embed_query", return_value=[0.1, 0.2, 0.3]):
        ks = KnowledgeSearch(provider="qdrant", collection_name="kb", embed_provider="sentence_transformers")
        result = ks.search("hi", context_name="kb", top_k=1)

    assert result["chunks"][0]["chunk_id"] == "c1"
    assert result["_receipt"]["items"][0]["score"] == 0.8


def test_unsupported_provider_raises():
    with patch.object(gateway, "_embed_query", return_value=[0.1]):
        ks = KnowledgeSearch(provider="not_a_real_provider", collection_name="kb")
        with pytest.raises(ValueError, match="unsupported provider"):
            ks.search("hi", context_name="kb")


def test_unsupported_embed_provider_raises():
    ks = KnowledgeSearch(provider="chromadb", collection_name="kb", embed_provider="not_a_real_embed_provider")
    with pytest.raises(ValueError, match="unsupported embed_provider"):
        ks.search("hi", context_name="kb")


def test_rerank_with_priors_reorders_results():
    results = [
        {"id": "c1", "metadata": {"chunk_id": "c1", "text": "a"}, "document": None, "score": 0.5},
        {"id": "c2", "metadata": {"chunk_id": "c2", "text": "b"}, "document": None, "score": 0.9},
    ]
    with patch.object(gateway, "_embed_query", return_value=[0.1]), \
         patch.object(gateway, "_query_vector_store", return_value=results):
        ks = KnowledgeSearch(provider="chromadb", collection_name="kb")
        # c1 has a strong positive prior despite a lower raw score -- should outrank c2.
        result = ks.search("q", context_name="kb", top_k=2, priors={"c1": {"prior": 1.0}, "c2": {"prior": 0.0}}, prior_weight=1.0)

    assert [c["chunk_id"] for c in result["chunks"]] == ["c1", "c2"]
