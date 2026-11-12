"""chroma_resource_key / qdrant_resource_key -- same precedence-based shape as
vector_store_writer's own retrofit and dataframe_to_snowflake's `resource_key` field: when set
for the active `provider`, a shared ChromaResourceComponent/QdrantResourceComponent client
(resources/chroma_resource, resources/qdrant_resource) is used via its get_client() instead of
building a fresh client from connection_string; left unset, nothing changes.

Covers both call sites that build a chromadb/qdrant client: the main query loop, AND
track_retrieval_count's write-back path (_increment_retrieval_counts) -- a second, easy-to-miss
client-construction site that also needed the same resource_key wired through.

dagster_chroma/dagster_qdrant aren't installed in this test env, so these tests stand in a
minimal fake object exposing the same get_client() contextmanager interface those resources'
own build_defs() wires up (verified against the real dagster-chroma==0.0.5 / dagster-
qdrant==0.0.4 source). `dg.materialize()` is real throughout; asset-level
`required_resource_keys` wiring is implicitly checked too, since accessing an undeclared resource
key via `context.resources` would raise at runtime. pinecone/faiss/s3vectors/lancedb are
untouched by this change and are not re-tested here.
"""
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent


class _FakeChromaCollection:
    def __init__(self, vectors):
        self._vectors = vectors  # id -> (embedding, document, metadata)
        self.update_calls = []

    def query(self, query_embeddings=None, query_texts=None, n_results=10):
        ids = list(self._vectors.keys())[:n_results]
        return {
            "ids": [ids],
            "documents": [[self._vectors[i][1] for i in ids]],
            "metadatas": [[self._vectors[i][2] for i in ids]],
            "distances": [[0.1 for _ in ids]],
        }

    def update(self, ids, metadatas):
        self.update_calls.append((ids, metadatas))
        for i, md in zip(ids, metadatas):
            emb, doc, _ = self._vectors[i]
            self._vectors[i] = (emb, doc, md)


class _FakeChromaClient:
    def __init__(self, vectors):
        self.collection = _FakeChromaCollection(vectors)
        self.get_collection_calls = []

    def get_collection(self, name):
        self.get_collection_calls.append(name)
        return self.collection


class _FakeChromaResource:
    def __init__(self, vectors):
        self.client = _FakeChromaClient(vectors)
        self.entered = 0

    @contextmanager
    def get_client(self):
        self.entered += 1
        yield self.client


class _FakeQdrantHit:
    def __init__(self, id, score, payload):
        self.id = id
        self.score = score
        self.payload = payload


class _FakeQdrantClient:
    def __init__(self, hits):
        self._hits = hits
        self.set_payload_calls = []

    def search(self, collection_name, query_vector, limit):
        return self._hits[:limit]

    def set_payload(self, **kw):
        self.set_payload_calls.append(kw)


class _FakeQdrantResource:
    def __init__(self, hits):
        self.client = _FakeQdrantClient(hits)
        self.entered = 0

    @contextmanager
    def get_client(self):
        self.entered += 1
        yield self.client


def _materialize(comp, df, resources):
    @dg.asset(name="queries")
    def queries():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([queries, *defs.assets], resources=resources, raise_on_error=True)


def test_chroma_resource_key_honored_instead_of_connection_string(monkeypatch):
    import chromadb

    def _boom(*a, **kw):
        raise AssertionError("chromadb.PersistentClient should not be constructed when chroma_resource_key is set")

    monkeypatch.setattr(chromadb, "PersistentClient", _boom)

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="chromadb", collection_name="docs",
        upstream_asset_key="queries", chroma_resource_key="chroma_resource",
        connection_string="/should/not/be/used", top_k=5,
    )
    fake = _FakeChromaResource({"doc_0": ([0.1, 0.2], "hello", {"a": 1})})
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})

    result = _materialize(comp, df, resources={"chroma_resource": fake})
    assert result.success
    assert fake.entered == 1
    assert fake.client.get_collection_calls == ["docs"]


def test_chroma_resource_key_also_used_by_track_retrieval_count(monkeypatch):
    import chromadb

    monkeypatch.setattr(chromadb, "PersistentClient", lambda *a, **kw: (_ for _ in ()).throw(
        AssertionError("chromadb.PersistentClient should not be constructed when chroma_resource_key is set")
    ))

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="chromadb", collection_name="docs",
        upstream_asset_key="queries", chroma_resource_key="chroma_resource",
        top_k=5, track_retrieval_count=True,
    )
    fake = _FakeChromaResource({"doc_0": ([0.1, 0.2], "hello", {"a": 1})})
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})

    result = _materialize(comp, df, resources={"chroma_resource": fake})
    assert result.success
    # entered twice: once for the query itself, once for the retrieval-count write-back
    assert fake.entered == 2
    assert len(fake.client.collection.update_calls) == 1
    ids, metadatas = fake.client.collection.update_calls[0]
    assert metadatas[0]["retrieval_count"] == 1


def test_chroma_resource_key_unset_uses_inline_connection_string(tmp_path):
    import chromadb

    client = chromadb.PersistentClient(path=str(tmp_path / "chroma"))
    collection = client.get_or_create_collection(name="docs")
    collection.add(ids=["doc_0"], embeddings=[[0.1, 0.2]], documents=["hello"])

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="chromadb", collection_name="docs",
        upstream_asset_key="queries", connection_string=str(tmp_path / "chroma"), top_k=5,
    )
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})
    result = _materialize(comp, df, resources={})
    assert result.success


def test_qdrant_resource_key_honored_instead_of_connection_string(monkeypatch):
    import qdrant_client

    def _boom(*a, **kw):
        raise AssertionError("QdrantClient should not be constructed when qdrant_resource_key is set")

    monkeypatch.setattr(qdrant_client, "QdrantClient", _boom)

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="qdrant", collection_name="docs",
        upstream_asset_key="queries", qdrant_resource_key="qdrant_resource",
        connection_string="http://should-not-be-used", top_k=5,
    )
    fake = _FakeQdrantResource([_FakeQdrantHit("doc_0", 0.9, {"a": 1})])
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})

    result = _materialize(comp, df, resources={"qdrant_resource": fake})
    assert result.success
    assert fake.entered == 1


def test_qdrant_resource_key_also_used_by_track_retrieval_count(monkeypatch):
    import qdrant_client

    def _boom(*a, **kw):
        raise AssertionError("QdrantClient should not be constructed when qdrant_resource_key is set")

    monkeypatch.setattr(qdrant_client, "QdrantClient", _boom)

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="qdrant", collection_name="docs",
        upstream_asset_key="queries", qdrant_resource_key="qdrant_resource",
        top_k=5, track_retrieval_count=True,
    )
    fake = _FakeQdrantResource([_FakeQdrantHit("doc_0", 0.9, {"a": 1})])
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})

    result = _materialize(comp, df, resources={"qdrant_resource": fake})
    assert result.success
    assert fake.entered == 2
    assert len(fake.client.set_payload_calls) == 1


def test_qdrant_resource_key_unset_uses_inline_qdrant_client(monkeypatch):
    import qdrant_client

    calls = {}

    class _FakeInlineClient:
        def search(self, collection_name, query_vector, limit):
            return [_FakeQdrantHit("doc_0", 0.9, {"a": 1})]

    def _fake_ctor(**kw):
        calls["ctor"] = kw
        return _FakeInlineClient()

    monkeypatch.setattr(qdrant_client, "QdrantClient", _fake_ctor)

    comp = VectorStoreQueryComponent(
        asset_name="results", provider="qdrant", collection_name="docs",
        upstream_asset_key="queries", connection_string="http://localhost:1234", top_k=5,
    )
    df = pd.DataFrame({"embedding": [[0.1, 0.2]]})
    result = _materialize(comp, df, resources={})
    assert result.success
    assert calls["ctor"]["url"] == "http://localhost:1234"


def test_pinecone_faiss_lancedb_untouched_by_new_fields():
    VectorStoreQueryComponent(asset_name="results", provider="faiss", collection_name="idx", upstream_asset_key="queries")
    VectorStoreQueryComponent(asset_name="results", provider="pinecone", collection_name="idx", upstream_asset_key="queries")
    VectorStoreQueryComponent(asset_name="results", provider="lancedb", collection_name="idx", upstream_asset_key="queries")
