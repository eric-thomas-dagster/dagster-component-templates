"""Tests for ChromadbAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the OpenAI embeddings call (a real in-memory chromadb collection receives the upserts) is mocked."""
import sqlite3
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def sqlite_url(tmp_path, monkeypatch):
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE docs (id INTEGER, body TEXT, score REAL)")
    conn.executemany("INSERT INTO docs VALUES (?, ?, ?)", [(1, "alpha", 0.5), (2, "beta", 1.5), (3, "gamma", 2.5)])
    conn.commit()
    conn.close()
    monkeypatch.setenv("SRC_DB_URL", f"sqlite:///{db}")
    return f"sqlite:///{db}"


SOURCE = {"kind": "warehouse_query", "database_url_env_var": "SRC_DB_URL",
          "sql": "SELECT id, body, score FROM docs WHERE id <= 2 ORDER BY id"}

def test_source_warehouse_query_embeds_and_upserts(mod, sqlite_url, monkeypatch):
    import chromadb
    import openai
    client = chromadb.EphemeralClient()
    monkeypatch.setattr(chromadb, "HttpClient", lambda **kw: client)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class FakeOpenAI:
        def __init__(self, **kw):
            self.embeddings = types.SimpleNamespace(create=lambda input, model: types.SimpleNamespace(
                data=[types.SimpleNamespace(embedding=[float(len(t)), 1.0, 0.0]) for t in input]))

    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    comp = mod.ChromadbAssetComponent(asset_name="out", collection_name="dual_input_test", id_column="id",
                                      text_column="body", metadata_columns=["score"], source=SOURCE)
    result = dg.materialize(list(comp.build_defs(context=None).assets))
    assert result.success
    got = client.get_collection("dual_input_test").get(include=["documents", "metadatas"])
    assert sorted(got["ids"]) == ["1", "2"]
    assert sorted(got["documents"]) == ["alpha", "beta"]


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", collection_name="c", id_column="id", text_column="body")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ChromadbAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ChromadbAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
