"""chroma_resource_key / qdrant_resource_key -- precedence-based (not exactly-one-of), same
shape as dataframe_to_snowflake's own `resource_key` field: when set for the active `provider`,
a shared ChromaResourceComponent/QdrantResourceComponent client (resources/chroma_resource,
resources/qdrant_resource) is used via its get_client() instead of building a fresh client from
connection_string; left unset, nothing changes (the pre-existing inline connection_string path).

dagster_chroma/dagster_qdrant aren't installed in this test env, so these tests stand in a
minimal fake object exposing the same get_client() contextmanager interface those resources'
own build_defs() wires up -- verified against the real dagster-chroma==0.0.5 / dagster-
qdrant==0.0.4 source: both resources' get_client() is a @contextmanager yielding the real
chromadb/qdrant client (`ChromaResource.get_client`, `QdrantResource.get_client`).

`dg.materialize()` is real throughout: asset-level `required_resource_keys` wiring is
implicitly checked too, since accessing an undeclared resource key via `context.resources` would
raise at runtime. pinecone/faiss/s3vectors are untouched by this change -- their own existing
tests (test_s3vectors_provider.py) and the component's own validators aren't re-tested here.
"""
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreWriterComponent = mod.VectorStoreWriterComponent


class _FakeChromaCollection:
    def __init__(self):
        self.upsert_calls = []

    def upsert(self, **kw):
        self.upsert_calls.append(kw)


class _FakeChromaClient:
    def __init__(self):
        self.collections = {}
        self.get_or_create_collection_calls = []

    def get_or_create_collection(self, name):
        self.get_or_create_collection_calls.append(name)
        return self.collections.setdefault(name, _FakeChromaCollection())


class _FakeChromaResource:
    def __init__(self):
        self.client = _FakeChromaClient()
        self.entered = 0

    @contextmanager
    def get_client(self):
        self.entered += 1
        yield self.client


class _FakeQdrantClient:
    def __init__(self):
        self.upsert_calls = []
        self.created = []
        self.collections = set()

    def get_collection(self, name):
        if name not in self.collections:
            raise Exception("collection not found")

    def create_collection(self, **kw):
        self.created.append(kw)
        self.collections.add(kw["collection_name"])

    def upsert(self, **kw):
        self.upsert_calls.append(kw)


class _FakeQdrantResource:
    def __init__(self):
        self.client = _FakeQdrantClient()
        self.entered = 0

    @contextmanager
    def get_client(self):
        self.entered += 1
        yield self.client


def _materialize(comp, df, resources):
    @dg.asset(name="embeddings")
    def embeddings():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([embeddings, *defs.assets], resources=resources, raise_on_error=True)


def test_chroma_resource_key_honored_instead_of_connection_string(monkeypatch):
    import chromadb

    def _boom(*a, **kw):
        raise AssertionError("chromadb.PersistentClient should not be constructed when chroma_resource_key is set")

    monkeypatch.setattr(chromadb, "PersistentClient", _boom)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="chromadb", collection_name="docs",
        upstream_asset_key="embeddings", chroma_resource_key="chroma_resource",
        connection_string="/should/not/be/used",
    )
    fake = _FakeChromaResource()
    df = pd.DataFrame({"embedding": [[0.1, 0.2], [0.3, 0.4]], "text": ["a", "b"]})

    result = _materialize(comp, df, resources={"chroma_resource": fake})
    assert result.success
    assert fake.entered == 1
    assert fake.client.get_or_create_collection_calls == ["docs"]
    assert len(fake.client.collections["docs"].upsert_calls) == 1


def test_chroma_resource_key_unset_uses_inline_connection_string(tmp_path):
    # Real chromadb.PersistentClient against a tmp_path -- no resources= needed, confirming the
    # unset path is completely unaffected by the new field (same as before this retrofit).
    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="chromadb", collection_name="docs",
        upstream_asset_key="embeddings", connection_string=str(tmp_path / "chroma"),
    )
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(comp, df, resources={})
    assert result.success


def test_qdrant_resource_key_honored_instead_of_connection_string(monkeypatch):
    import qdrant_client

    def _boom(*a, **kw):
        raise AssertionError("QdrantClient should not be constructed when qdrant_resource_key is set")

    monkeypatch.setattr(qdrant_client, "QdrantClient", _boom)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="qdrant", collection_name="docs",
        upstream_asset_key="embeddings", qdrant_resource_key="qdrant_resource",
        connection_string="http://should-not-be-used",
    )
    fake = _FakeQdrantResource()
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})

    result = _materialize(comp, df, resources={"qdrant_resource": fake})
    assert result.success
    assert fake.entered == 1
    assert len(fake.client.upsert_calls) == 1
    assert fake.client.created[0]["collection_name"] == "docs"


def test_qdrant_resource_key_unset_uses_inline_qdrant_client(monkeypatch):
    # qdrant_resource_key unset -- the pre-existing inline QdrantClient(url=...) path, monkeypatched
    # only to avoid a real network connection attempt in CI.
    import qdrant_client

    calls = {}

    class _FakeInlineClient:
        def get_collection(self, name):
            raise Exception("collection not found")

        def create_collection(self, **kw):
            calls["create"] = kw

        def upsert(self, **kw):
            calls.setdefault("upsert", []).append(kw)

    def _fake_ctor(**kw):
        calls["ctor"] = kw
        return _FakeInlineClient()

    monkeypatch.setattr(qdrant_client, "QdrantClient", _fake_ctor)

    comp = VectorStoreWriterComponent(
        asset_name="vecs", provider="qdrant", collection_name="docs",
        upstream_asset_key="embeddings", connection_string="http://localhost:1234",
    )
    df = pd.DataFrame({"embedding": [[0.1, 0.2]], "text": ["a"]})
    result = _materialize(comp, df, resources={})
    assert result.success
    assert calls["ctor"]["url"] == "http://localhost:1234"
    assert "upsert" in calls


def test_pinecone_faiss_untouched_by_new_fields():
    # New provider-specific fields are all optional; every other provider's validators and
    # client-construction keep working exactly as before.
    VectorStoreWriterComponent(asset_name="vecs", provider="faiss", collection_name="idx", upstream_asset_key="embeddings")
    VectorStoreWriterComponent(asset_name="vecs", provider="pinecone", collection_name="idx", upstream_asset_key="embeddings")
