"""embed op's reuse_from, generalized beyond chromadb (its original -- and, until now, only
supported -- provider) to qdrant/pinecone/lancedb, each with their own "fetch by id, with
vector and metadata" API. qdrant/pinecone are exercised with their SDK client classes mocked
(the one paid-API boundary for those two providers, same convention as write_vector_store's
own tests); lancedb is embedded/local, exercised for real.
"""
import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from .conftest import load_component_module, requires_lancedb, requires_pinecone, requires_qdrant_client


@pytest.fixture()
def mod():
    return load_component_module()


def _df(chunk_ids, texts):
    return pd.DataFrame({
        "chunk_id": chunk_ids,
        "chunk_text": texts,
        "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts],
    })


class _FakeCtx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        warning = info


def test_unsupported_provider_raises_clearly(mod):
    df = _df(["c1"], ["a"])
    with pytest.raises(ValueError, match="unsupported provider"):
        mod._do_embed(df, {"provider": "sentence_transformers", "model": "m", "reuse_from": {"provider": "not_real"}}, _FakeCtx())


@requires_qdrant_client
def test_qdrant_reuse_from_mocked_sdk(mod):
    """Every chunk is already indexed and unchanged -- nothing new to compute, so this never
    calls _do_embed_raw at all (no real model needed)."""
    df = _df(["c1"], ["unchanged text"])
    import uuid as _uuid
    point_id_c1 = str(_uuid.uuid5(_uuid.NAMESPACE_URL, "c1"))
    fake_point = MagicMock(id=point_id_c1, vector=[0.1, 0.2], payload={"content_hash": df.iloc[0]["content_hash"], "embedding_model": "m"})
    fake_client = MagicMock()
    fake_client.retrieve.return_value = [fake_point]

    with patch("qdrant_client.QdrantClient", return_value=fake_client):
        out = mod._do_embed(df, {
            "provider": "sentence_transformers", "model": "m",
            "reuse_from": {"provider": "qdrant", "collection_name": "kb"},
        }, _FakeCtx())

    assert out.attrs["stats"]["embeddings_reused"] == 1
    assert out.attrs["stats"]["embeddings_computed"] == 0
    assert list(out["embedding"])[0] == pytest.approx([0.1, 0.2])


@requires_pinecone
def test_pinecone_reuse_from_mocked_sdk(mod):
    df = _df(["c1"], ["unchanged text"])
    fake_index = MagicMock()
    fake_vec = MagicMock(values=[0.1, 0.2], metadata={"content_hash": df.iloc[0]["content_hash"], "embedding_model": "m"})
    fake_index.fetch.return_value = MagicMock(vectors={"c1": fake_vec})
    fake_pc = MagicMock()
    fake_pc.Index.return_value = fake_index

    with patch("pinecone.Pinecone", return_value=fake_pc):
        out = mod._do_embed(df, {
            "provider": "sentence_transformers", "model": "m",
            "reuse_from": {"provider": "pinecone", "collection_name": "kb"},
        }, _FakeCtx())

    assert out.attrs["stats"]["embeddings_reused"] == 1
    assert list(out["embedding"])[0] == pytest.approx([0.1, 0.2])


@requires_lancedb
def test_lancedb_reuse_from_real(mod, tmp_path):
    import lancedb

    digest_unchanged = hashlib.sha256(b"unchanged text").hexdigest()
    db = lancedb.connect(str(tmp_path / "lance"))
    seed = pd.DataFrame({
        "chunk_id": ["c1"], "vector": [[0.1, 0.2]],
        "content_hash": [digest_unchanged], "embedding_model": ["m"],
    })
    db.create_table("kb", seed, mode="overwrite")

    df = _df(["c1"], ["unchanged text"])
    out = mod._do_embed(df, {
        "provider": "sentence_transformers", "model": "m",
        "reuse_from": {"provider": "lancedb", "collection_name": "kb", "connection_string": str(tmp_path / "lance")},
    }, _FakeCtx())

    assert out.attrs["stats"]["embeddings_reused"] == 1
    # lancedb stores vectors as float32 -- a real precision artifact on round-trip, not a bug.
    assert list(out["embedding"])[0] == pytest.approx([0.1, 0.2], abs=1e-6)
    # Real bug: embedding_model used to be hardcoded None in this all-reused case (nothing was
    # freshly embedded this run) -- the configured model is still a known, static fact and must
    # always be reported, not just when a fresh embedding call happened to run.
    assert out.attrs["stats"]["embedding_model"] == "m"
    assert out.attrs["stats"]["embeddings_computed"] == 0


def test_all_reused_still_reports_the_provider_default_model_name(mod, tmp_path):
    """Same all-reused scenario, but `model` is left unset -- embedding_model must report the
    resolved provider default ('all-MiniLM-L6-v2' for sentence_transformers), not None."""
    import lancedb

    digest_unchanged = hashlib.sha256(b"unchanged text").hexdigest()
    db = lancedb.connect(str(tmp_path / "lance"))
    seed = pd.DataFrame({
        "chunk_id": ["c1"], "vector": [[0.1, 0.2]],
        "content_hash": [digest_unchanged], "embedding_model": ["all-MiniLM-L6-v2"],
    })
    db.create_table("kb", seed, mode="overwrite")

    df = _df(["c1"], ["unchanged text"])
    out = mod._do_embed(df, {
        "provider": "sentence_transformers",
        "reuse_from": {"provider": "lancedb", "collection_name": "kb", "connection_string": str(tmp_path / "lance")},
    }, _FakeCtx())

    assert out.attrs["stats"]["embedding_model"] == "all-MiniLM-L6-v2"
