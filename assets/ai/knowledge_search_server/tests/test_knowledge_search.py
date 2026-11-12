"""KnowledgeSearchResource.search(): chromadb + sentence_transformers is exercised for real
(both free/local, no paid API, no mocking needed) -- real embeddings, a real chromadb
collection, a real query. pinecone/qdrant are covered with their SDK client classes mocked
(the one paid-API boundary for those two providers), skipped when the SDK itself isn't
installed -- `patch("pinecone.Pinecone", ...)`/`patch("qdrant_client.QdrantClient", ...)`
still need the real package importable to resolve the dotted patch target."""
from unittest.mock import MagicMock, patch

import pytest


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
def test_real_chromadb_search_returns_text_score_source_doc_id_chunk_id(mod, tmp_path):
    import chromadb
    from sentence_transformers import SentenceTransformer

    db_path = str(tmp_path / "chroma")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    texts = [
        "Our return policy is 30 days.",
        "The weather today is sunny and warm.",
    ]
    embeddings = model.encode(texts, normalize_embeddings=True).tolist()

    client = chromadb.PersistentClient(path=db_path)
    coll = client.get_or_create_collection(name="kb_test")
    coll.upsert(
        ids=["c1", "c2"],
        embeddings=embeddings,
        documents=texts,
        metadatas=[
            {"chunk_id": "c1", "source_id": "doc-1", "source_asset_key": "docs_kb"},
            {"chunk_id": "c2", "source_id": "doc-2", "source_asset_key": "docs_kb"},
        ],
    )

    resource = mod.KnowledgeSearchResource(provider="chromadb", connection_string=db_path, collection_name="kb_test")
    result = resource.search("what is your return policy?", top_k=2)

    assert result["chunks"][0]["chunk_id"] == "c1"
    assert result["chunks"][0]["text"] == texts[0]
    assert result["chunks"][0]["source"] == "docs_kb"
    assert result["chunks"][0]["doc_id"] == "doc-1"
    assert len(result["chunks"]) == 2


@requires_pinecone
def test_pinecone_provider_mocked_sdk(mod):
    fake_match = {"id": "c1", "score": 0.9, "metadata": {"chunk_id": "c1", "source_id": "doc-1", "source_asset_key": "docs_kb", "chunk_text": "hello"}}
    fake_index = MagicMock()
    fake_index.query.return_value = {"matches": [fake_match]}
    fake_pc = MagicMock()
    fake_pc.Index.return_value = fake_index

    with patch("pinecone.Pinecone", return_value=fake_pc), patch.object(mod, "_embed_query", return_value=[0.1, 0.2, 0.3]):
        resource = mod.KnowledgeSearchResource(provider="pinecone", collection_name="kb")
        result = resource.search("hello", top_k=1)

    assert result["chunks"][0]["chunk_id"] == "c1"
    assert result["chunks"][0]["text"] == "hello"
    assert result["chunks"][0]["score"] == 0.9
    assert result["chunks"][0]["doc_id"] == "doc-1"


@requires_qdrant_client
def test_qdrant_provider_mocked_sdk(mod):
    fake_hit = MagicMock(id="c1", score=0.8, payload={"chunk_id": "c1", "source_id": "doc-1", "source_asset_key": "docs_kb", "chunk_text": "hi"})
    fake_client = MagicMock()
    fake_client.search.return_value = [fake_hit]

    with patch("qdrant_client.QdrantClient", return_value=fake_client), patch.object(mod, "_embed_query", return_value=[0.1, 0.2, 0.3]):
        resource = mod.KnowledgeSearchResource(provider="qdrant", collection_name="kb")
        result = resource.search("hi", top_k=1)

    assert result["chunks"][0]["chunk_id"] == "c1"
    assert result["chunks"][0]["score"] == 0.8


def test_text_falls_back_to_metadata_key_when_no_document_field(mod):
    """pinecone/qdrant have no separate 'documents' field -- text comes from metadata under
    text_metadata_key (default 'chunk_text', matching write_vector_store's own default
    text_column) instead."""
    with patch.object(mod, "_embed_query", return_value=[0.1]), \
         patch.object(mod, "_query_vector_store", return_value=[
             {"id": "c1", "metadata": {"chunk_id": "c1", "chunk_text": "stored in metadata"}, "document": None, "score": 0.5},
         ]):
        resource = mod.KnowledgeSearchResource(provider="pinecone", collection_name="kb")
        result = resource.search("q", top_k=1)
    assert result["chunks"][0]["text"] == "stored in metadata"


def test_custom_text_metadata_key_respected(mod):
    with patch.object(mod, "_embed_query", return_value=[0.1]), \
         patch.object(mod, "_query_vector_store", return_value=[
             {"id": "c1", "metadata": {"chunk_id": "c1", "body": "custom column"}, "document": None, "score": 0.5},
         ]):
        resource = mod.KnowledgeSearchResource(provider="pinecone", collection_name="kb", text_metadata_key="body")
        result = resource.search("q", top_k=1)
    assert result["chunks"][0]["text"] == "custom column"


def test_unsupported_provider_raises(mod):
    """KnowledgeSearchResource.provider is a Literal, validated (and frozen -- it's a
    ConfigurableResource) at construction, so an invalid value can never reach .search()
    through the resource itself; this exercises the underlying helper function directly."""
    with pytest.raises(ValueError, match="unsupported provider"):
        mod._query_vector_store([0.1], provider="not_a_real_provider", connection_string=None, collection_name="kb", top_k=1, api_key_env_var=None)


def test_unsupported_embed_provider_raises(mod):
    with pytest.raises(ValueError, match="unsupported embed_provider"):
        mod._embed_query("hi", "not_a_real_embed_provider", None, None)
