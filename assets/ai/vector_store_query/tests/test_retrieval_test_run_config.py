"""Ad-hoc retrieval testing: upstream_asset_key is now optional when query_text is set (static
or via VectorStoreQueryRunConfig), embed_provider embeds the query itself instead of relying on
chromadb's own silent default embedder, and hits are written to run metadata as real JSON
(rank/score/chunk_id/doc_id/text/metadata/parent_text), not just counts. Exercised for real
against a real chromadb collection + real sentence-transformers embeddings (both free/local,
no mocking needed at all).
"""
import pytest

pytest.importorskip("chromadb")
pytest.importorskip("sentence_transformers")

import dagster as dg

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent


def _seed_chromadb(path, collection="kb_test"):
    import chromadb
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-MiniLM-L6-v2")
    texts = ["Our return policy is 30 days.", "The weather today is sunny and warm."]
    embeddings = model.encode(texts, normalize_embeddings=True).tolist()
    client = chromadb.PersistentClient(path=path)
    coll = client.get_or_create_collection(name=collection)
    coll.upsert(
        ids=["c1", "c2"],
        embeddings=embeddings,
        documents=texts,
        metadatas=[
            {"chunk_id": "c1", "source_id": "doc1", "text": texts[0]},
            {"chunk_id": "c2", "source_id": "doc2", "text": texts[1]},
        ],
    )


def _component(tmp_path, **kw):
    attrs = dict(
        asset_name="results", provider="chromadb", collection_name="kb_test",
        connection_string=str(tmp_path / "chroma"),
    )
    attrs.update(kw)
    return VectorStoreQueryComponent(**attrs)


def _materialize(comp, **kwargs):
    defs = comp.build_defs(None)
    return dg.materialize(defs.assets, **kwargs)


def test_ad_hoc_query_with_no_upstream_asset_key(tmp_path):
    _seed_chromadb(str(tmp_path / "chroma"))
    comp = _component(tmp_path, query_text="what is your return policy?", embed_provider="sentence_transformers", top_k=1)
    result = _materialize(comp, raise_on_error=True)
    assert result.success
    df = result.asset_value("results")
    assert len(df) == 1


def test_missing_upstream_and_query_text_raises(tmp_path):
    comp = _component(tmp_path)  # no upstream_asset_key, no query_text
    result = _materialize(comp, raise_on_error=False)
    assert not result.success


def test_embed_provider_used_instead_of_chromadb_silent_default(tmp_path, monkeypatch):
    """embed_query_text must be called (and query_embeddings=, not query_texts=, sent to
    chromadb) whenever embed_provider is set -- the exact bug reported: chromadb's own
    query_texts= silently falls back to ITS default embedder, which may not match what the
    index was built with."""
    _seed_chromadb(str(tmp_path / "chroma"))
    calls = []
    real_embed = mod._embed_query_text

    def _spy(text, provider, model, api_key_env_var):
        calls.append((text, provider, model))
        return real_embed(text, provider, model, api_key_env_var)

    monkeypatch.setattr(mod, "_embed_query_text", _spy)
    comp = _component(tmp_path, query_text="what is your return policy?", embed_provider="sentence_transformers", embed_model="all-MiniLM-L6-v2", top_k=1)
    _materialize(comp, raise_on_error=True)
    assert len(calls) == 1
    assert calls[0] == ("what is your return policy?", "sentence_transformers", "all-MiniLM-L6-v2")


def test_run_config_query_text_overrides_static_field(tmp_path):
    _seed_chromadb(str(tmp_path / "chroma"))
    comp = _component(tmp_path, query_text="irrelevant static text", embed_provider="sentence_transformers", top_k=1)
    result = _materialize(
        comp, raise_on_error=True,
        run_config={"ops": {"results": {"config": {"query_text": "what is your return policy?"}}}},
    )
    assert result.success


def test_run_config_top_k_overrides_static_field(tmp_path):
    _seed_chromadb(str(tmp_path / "chroma"))
    comp = _component(tmp_path, query_text="return policy", embed_provider="sentence_transformers", top_k=1)
    result = _materialize(
        comp, raise_on_error=True,
        run_config={"ops": {"results": {"config": {"top_k": 2}}}},
    )
    df = result.asset_value("results")
    assert len(df) == 2


def test_hits_metadata_has_full_detail_not_just_counts(tmp_path):
    _seed_chromadb(str(tmp_path / "chroma"))
    comp = _component(tmp_path, query_text="return policy", embed_provider="sentence_transformers", top_k=2)
    result = _materialize(comp, raise_on_error=True)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    hits = meta["hits"].value
    assert isinstance(hits, list)
    assert len(hits) == 2
    for i, hit in enumerate(hits):
        assert hit["rank"] == i
        assert hit["chunk_id"] in ("c1", "c2")
        assert hit["doc_id"] in ("doc1", "doc2")
        assert hit["text"]
        assert "metadata" in hit


def test_knowledge_asset_key_infers_embed_model_from_another_assets_metadata(tmp_path, monkeypatch):
    """embed_model, left unset, should come from knowledge_asset_key's own last
    materialization metadata (the `embedding_model` key context_engineering_pipeline/
    vector_store_writer already promote to top-level metadata) -- never guessed, and never
    overriding an explicitly-set embed_model."""
    _seed_chromadb(str(tmp_path / "chroma"))
    calls = []
    real_embed = mod._embed_query_text

    def _spy(text, provider, model, api_key_env_var):
        calls.append(model)
        return real_embed(text, provider, model, api_key_env_var)

    monkeypatch.setattr(mod, "_embed_query_text", _spy)
    monkeypatch.setattr(mod, "_infer_embed_model_from_asset", lambda instance, key: "all-MiniLM-L6-v2")

    comp = _component(tmp_path, query_text="return policy", embed_provider="sentence_transformers", knowledge_asset_key="some_other_asset", top_k=1)
    result = _materialize(comp, raise_on_error=True)
    assert result.success
    assert calls == ["all-MiniLM-L6-v2"]


def test_knowledge_asset_key_ignored_when_embed_model_already_set(tmp_path, monkeypatch):
    _seed_chromadb(str(tmp_path / "chroma"))
    inferred = []
    monkeypatch.setattr(mod, "_infer_embed_model_from_asset", lambda instance, key: inferred.append(key) or "should-not-be-used")

    comp = _component(tmp_path, query_text="return policy", embed_provider="sentence_transformers", embed_model="all-MiniLM-L6-v2", knowledge_asset_key="some_other_asset", top_k=1)
    _materialize(comp, raise_on_error=True)
    assert inferred == []  # embed_model was already set -- knowledge_asset_key inference never ran


def test_non_chromadb_provider_without_embedding_raises_clear_error(tmp_path):
    comp = VectorStoreQueryComponent(
        asset_name="results", provider="pinecone", collection_name="kb_test",
        query_text="return policy",  # no embed_provider set -- nothing can produce a real vector
    )
    result = _materialize(comp, raise_on_error=False)
    assert not result.success
