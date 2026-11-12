"""provider='s3vectors'/'faiss' -- the writer (vector_store_writer) already supports these two providers that
knowledge_search_server didn't; ported the same real query code vector_store_query already uses (and the same
code now lives, byte-identical, in resources/context_receipt_store -- see tests/test_drift.py)."""
import json

import dagster as dg
import pytest


def _component(mod, **kw):
    kw.setdefault("resource_key", "ks")
    kw.setdefault("collection_name", "kb")
    kw.setdefault("embed_provider", "sentence_transformers")
    return mod.KnowledgeSearchServerComponent(**kw)


def test_s3vectors_demo_mode_round_trips_against_a_real_writer_shaped_json_file(mod, tmp_path, monkeypatch):
    demo_path = tmp_path / "bucket_kb_s3vectors_demo.json"
    demo_path.write_text(json.dumps({
        "indexes": {
            "kb": {
                "vectors": {
                    "chunk-1": {"data": {"float32": [1.0, 0.0, 0.0]}, "metadata": {"chunk_text": "hello world", "chunk_id": "chunk-1"}},
                    "chunk-2": {"data": {"float32": [0.0, 1.0, 0.0]}, "metadata": {"chunk_text": "goodbye", "chunk_id": "chunk-2"}},
                }
            }
        }
    }))

    class _FakeVec(list):
        def tolist(self):
            return list(self)

    class _FakeTransformer:
        def encode(self, texts, normalize_embeddings=True, show_progress_bar=False):
            return [_FakeVec([1.0, 0.0, 0.0])]  # matches chunk-1 exactly

    monkeypatch.setitem(__import__("sys").modules, "sentence_transformers", type(__import__("sys"))("sentence_transformers"))
    import sentence_transformers
    sentence_transformers.SentenceTransformer = lambda name: _FakeTransformer()

    component = _component(
        mod, provider="s3vectors", vector_bucket_name="bucket", connection_string=str(demo_path), demo_mode=True,
    )
    defs = component.build_defs(None)
    result = defs.resources["ks"].search("hello", top_k=1)
    assert result["chunks"][0]["chunk_id"] == "chunk-1"
    assert result["chunks"][0]["text"] == "hello world"


def test_s3vectors_requires_vector_bucket_name(mod):
    with pytest.raises(ValueError, match="requires vector_bucket_name"):
        _component(mod, provider="s3vectors").build_defs(None)


def test_s3vectors_non_demo_requires_aws_region(mod):
    with pytest.raises(ValueError, match="requires aws_region"):
        _component(mod, provider="s3vectors", vector_bucket_name="bucket", demo_mode=False).build_defs(None)


def test_faiss_is_a_valid_provider_choice(mod):
    """Load-time acceptance only (a real faiss index file is exercised in vector_store_query's own tests) --
    confirms the Literal widened and the query dispatch has a real faiss branch, not just a config no-op."""
    component = _component(mod, provider="faiss")
    defs = component.build_defs(None)
    assert defs.resources["ks"].provider == "faiss"
    import inspect
    assert 'provider == "faiss"' in inspect.getsource(mod._query_vector_store)
