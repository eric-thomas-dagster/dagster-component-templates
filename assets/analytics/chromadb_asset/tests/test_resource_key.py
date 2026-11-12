"""chroma_resource_key -- same precedence-based shape as vector_store_writer/vector_store_query's
own retrofit and dataframe_to_snowflake's `resource_key` field: when set, a shared
ChromaResourceComponent client (resources/chroma_resource) is used via its get_client() instead
of building a fresh chromadb.HttpClient from chroma_host/chroma_port/chroma_api_key_env_var; left
unset, nothing changes (same inline HttpClient path test_source_warehouse_query.py already
exercises).

dagster_chroma isn't installed in this test env, so this test stands in a minimal fake object
exposing the same get_client() contextmanager interface ChromaResourceComponent.build_defs()
wires up (verified against the real dagster-chroma==0.0.5 source: ChromaResource.get_client() is
a @contextmanager yielding the real chromadb client). `dg.materialize()` is real throughout,
against a real in-memory chromadb.EphemeralClient (only the OpenAI embeddings call is mocked,
matching this component's existing test convention).
"""
import types
from contextlib import contextmanager

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
ChromadbAssetComponent = mod.ChromadbAssetComponent


class _FakeChromaResource:
    def __init__(self, client):
        self.client = client
        self.entered = 0

    @contextmanager
    def get_client(self):
        self.entered += 1
        yield self.client


def _fake_openai(monkeypatch):
    import openai

    class FakeOpenAI:
        def __init__(self, **kw):
            self.embeddings = types.SimpleNamespace(create=lambda input, model: types.SimpleNamespace(
                data=[types.SimpleNamespace(embedding=[float(len(t)), 1.0, 0.0]) for t in input]))

    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")


def test_chroma_resource_key_honored_instead_of_http_client(monkeypatch):
    import chromadb

    def _boom(**kw):
        raise AssertionError("chromadb.HttpClient should not be constructed when chroma_resource_key is set")

    monkeypatch.setattr(chromadb, "HttpClient", _boom)
    _fake_openai(monkeypatch)

    real_client = chromadb.EphemeralClient()
    fake_resource = _FakeChromaResource(real_client)

    @dg.asset(name="docs_in")
    def docs_in():
        return pd.DataFrame({"id": [1, 2], "body": ["alpha", "beta"]})

    comp = ChromadbAssetComponent(
        asset_name="out", collection_name="resource_key_test", id_column="id", text_column="body",
        upstream_asset_key="docs_in", chroma_resource_key="chroma_resource",
    )
    defs = comp.build_defs(context=None)
    result = dg.materialize([docs_in, *defs.assets], resources={"chroma_resource": fake_resource}, raise_on_error=True)
    assert result.success
    assert fake_resource.entered == 1
    got = real_client.get_collection("resource_key_test").get(include=["documents"])
    assert sorted(got["ids"]) == ["1", "2"]
    assert sorted(got["documents"]) == ["alpha", "beta"]


def test_chroma_resource_key_unset_uses_inline_http_client(monkeypatch):
    import chromadb

    client = chromadb.EphemeralClient()
    monkeypatch.setattr(chromadb, "HttpClient", lambda **kw: client)
    _fake_openai(monkeypatch)

    @dg.asset(name="docs_in")
    def docs_in():
        return pd.DataFrame({"id": [1], "body": ["gamma"]})

    comp = ChromadbAssetComponent(
        asset_name="out", collection_name="inline_test", id_column="id", text_column="body",
        upstream_asset_key="docs_in",
    )
    defs = comp.build_defs(context=None)
    result = dg.materialize([docs_in, *defs.assets], raise_on_error=True)
    assert result.success
    got = client.get_collection("inline_test").get(include=["documents"])
    assert got["documents"] == ["gamma"]
