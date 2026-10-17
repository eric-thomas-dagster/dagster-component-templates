"""Tests for OllamaInferenceAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the HTTP call to the Ollama server (requests.post) is mocked."""
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

def test_source_warehouse_query_runs_llm_over_sql_rows(mod, sqlite_url, monkeypatch):
    import requests
    sent = []

    class FakeResp:
        def __init__(self, prompt):
            self._p = prompt
        def raise_for_status(self):
            pass
        def json(self):
            return {"response": f"echo:{self._p}"}

    def fake_post(url, json=None, timeout=None, **kw):
        sent.append(json["prompt"])
        return FakeResp(json["prompt"])

    monkeypatch.setattr(requests, "post", fake_post)
    comp = mod.OllamaInferenceAssetComponent(asset_name="out", prompt_template="Say: {body}", source=SOURCE)
    result = dg.materialize(list(comp.build_defs(context=None).assets))
    assert result.success
    df = result.output_for_node("out")
    assert list(df["id"]) == [1, 2]
    assert list(df["llm_response"]) == ["echo:Say: alpha", "echo:Say: beta"]
    assert sent == ["Say: alpha", "Say: beta"]


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", prompt_template="Say: {body}")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.OllamaInferenceAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.OllamaInferenceAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
