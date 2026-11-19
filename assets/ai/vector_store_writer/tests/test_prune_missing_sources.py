"""id_column (stable per-row ids instead of positional doc_{idx}) + prune_missing_sources (delete
anything in the collection whose id_column value is not in this run's data) -- chromadb, pinecone,
qdrant only; faiss (no delete API) and s3vectors (not yet implemented) are refused at config time.

Real chromadb/qdrant_client/pinecone packages are installed; only their client constructors are
monkeypatched to return fakes, same pattern as test_resource_key.py -- dg.materialize() is real
throughout."""
from types import SimpleNamespace

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreWriterComponent = mod.VectorStoreWriterComponent


def _materialize(comp, df):
    @dg.asset(name="embeddings")
    def embeddings():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([embeddings, *defs.assets], raise_on_error=True)


def _df(rows):
    return pd.DataFrame(rows)


# ─── chromadb ────────────────────────────────────────────────────────────────────────────────────

class _FakeChromaCollection:
    def __init__(self, existing_ids):
        self.store = {i: True for i in existing_ids}
        self.delete_calls = []

    def upsert(self, ids, **kw):
        for i in ids:
            self.store[i] = True

    def get(self, include=None):
        return {"ids": list(self.store)}

    def delete(self, ids):
        self.delete_calls.append(list(ids))
        for i in ids:
            self.store.pop(i, None)


class _FakeChromaClient:
    def __init__(self, existing_ids):
        self.collection = _FakeChromaCollection(existing_ids)

    def get_or_create_collection(self, name):
        return self.collection


def test_chromadb_id_column_and_prune_deletes_vanished_sources(monkeypatch):
    import chromadb

    fake_client = _FakeChromaClient(existing_ids=["keep-1", "gone-1", "gone-2"])
    monkeypatch.setattr(chromadb, "PersistentClient", lambda path=None: fake_client)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="chromadb", collection_name="docs",
        upstream_asset_key="embeddings", connection_string="/unused",
        id_column="doc_id", prune_missing_sources=True,
    )
    df = _df([{"doc_id": "keep-1", "embedding": [0.1, 0.2], "text": "a"}])
    result = _materialize(comp, df)
    assert result.success
    assert fake_client.collection.delete_calls == [["gone-1", "gone-2"]] or sorted(fake_client.collection.delete_calls[0]) == ["gone-1", "gone-2"]
    assert set(fake_client.collection.store) == {"keep-1"}


def test_chromadb_prune_false_leaves_stale_vectors(monkeypatch):
    import chromadb

    fake_client = _FakeChromaClient(existing_ids=["keep-1", "stale-1"])
    monkeypatch.setattr(chromadb, "PersistentClient", lambda path=None: fake_client)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="chromadb", collection_name="docs",
        upstream_asset_key="embeddings", connection_string="/unused", id_column="doc_id",
    )
    df = _df([{"doc_id": "keep-1", "embedding": [0.1, 0.2], "text": "a"}])
    result = _materialize(comp, df)
    assert result.success
    assert fake_client.collection.delete_calls == []
    assert "stale-1" in fake_client.collection.store


# ─── pinecone ────────────────────────────────────────────────────────────────────────────────────

class _FakePineconeIndex:
    def __init__(self, existing_ids):
        self.ids = set(existing_ids)
        self.upsert_calls = []
        self.delete_calls = []

    def upsert(self, vectors):
        self.upsert_calls.append(vectors)
        self.ids.update(v["id"] for v in vectors)

    def list(self):
        yield SimpleNamespace(vectors=[SimpleNamespace(id=i) for i in self.ids])

    def delete(self, ids):
        self.delete_calls.append(list(ids))
        self.ids.difference_update(ids)


def test_pinecone_id_column_and_prune_deletes_vanished_sources(monkeypatch):
    import pinecone

    fake_index = _FakePineconeIndex(existing_ids=["keep-1", "gone-1"])
    monkeypatch.setattr(pinecone, "Pinecone", lambda api_key=None: SimpleNamespace(Index=lambda name: fake_index))

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="pinecone", collection_name="docs",
        upstream_asset_key="embeddings", id_column="doc_id", prune_missing_sources=True,
    )
    df = _df([{"doc_id": "keep-1", "embedding": [0.1, 0.2], "text": "a"}])
    result = _materialize(comp, df)
    assert result.success
    assert fake_index.delete_calls == [["gone-1"]]
    assert fake_index.ids == {"keep-1"}


# ─── qdrant ──────────────────────────────────────────────────────────────────────────────────────

class _FakeQdrantRecord:
    def __init__(self, id_):
        self.id = id_


class _FakeQdrantClient:
    def __init__(self, existing_ids):
        self.ids = set(existing_ids)
        self.collections = {"docs"}
        self.delete_calls = []

    def get_collection(self, name):
        if name not in self.collections:
            raise Exception("not found")

    def create_collection(self, **kw):
        self.collections.add(kw["collection_name"])

    def upsert(self, collection_name, points):
        for p in points:
            self.ids.add(p.id)

    def scroll(self, collection_name, with_payload=False, with_vectors=False, offset=None, limit=256):
        return [_FakeQdrantRecord(i) for i in self.ids], None

    def delete(self, collection_name, points_selector):
        ids = list(points_selector.points)
        self.delete_calls.append(ids)
        self.ids.difference_update(ids)


def test_qdrant_id_column_and_prune_deletes_vanished_sources(monkeypatch):
    import qdrant_client

    stable_id = mod._qdrant_point_id("gone-1")
    fake_client = _FakeQdrantClient(existing_ids=[mod._qdrant_point_id("keep-1"), stable_id])
    monkeypatch.setattr(qdrant_client, "QdrantClient", lambda **kw: fake_client)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="qdrant", collection_name="docs",
        upstream_asset_key="embeddings", connection_string="http://localhost:1234",
        id_column="doc_id", prune_missing_sources=True,
    )
    df = _df([{"doc_id": "keep-1", "embedding": [0.1, 0.2], "text": "a"}])
    result = _materialize(comp, df)
    assert result.success
    assert fake_client.delete_calls == [[stable_id]]
    assert fake_client.ids == {mod._qdrant_point_id("keep-1")}


def test_qdrant_point_id_is_deterministic_and_handles_ints_and_uuids():
    assert mod._qdrant_point_id("abc-123") == mod._qdrant_point_id("abc-123")
    assert mod._qdrant_point_id(42) == 42
    assert mod._qdrant_point_id("42") == 42
    uid = "550e8400-e29b-41d4-a716-446655440000"
    assert mod._qdrant_point_id(uid) == uid


# ─── validation ──────────────────────────────────────────────────────────────────────────────────

def test_prune_missing_sources_requires_id_column():
    with pytest.raises(ValueError, match="id_column"):
        VectorStoreWriterComponent(
            asset_name="vecs", provider="chromadb", collection_name="docs",
            upstream_asset_key="embeddings", prune_missing_sources=True,
        )


def test_prune_missing_sources_rejected_for_faiss():
    with pytest.raises(ValueError, match="faiss"):
        VectorStoreWriterComponent(
            asset_name="vecs", provider="faiss", collection_name="docs",
            upstream_asset_key="embeddings", id_column="doc_id", prune_missing_sources=True,
        )


def test_prune_missing_sources_rejected_for_s3vectors():
    with pytest.raises(ValueError, match="s3vectors"):
        VectorStoreWriterComponent(
            asset_name="vecs", provider="s3vectors", collection_name="docs",
            upstream_asset_key="embeddings", id_column="doc_id", prune_missing_sources=True,
            vector_bucket_name="b", aws_region="us-east-1",
        )
