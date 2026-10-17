"""Tests for LiteLLMInferenceAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the litellm.completion call (the paid LLM request) is mocked."""
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
    import litellm
    prompts = []

    def fake_completion(model, messages, **kw):
        prompts.append(messages[-1]["content"])
        msg = types.SimpleNamespace(content=f"echo:{messages[-1]['content']}")
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])

    monkeypatch.setattr(litellm, "completion", fake_completion)
    comp = mod.LiteLLMInferenceAssetComponent(asset_name="out", prompt_template="Say: {body}", model="gpt-4o", source=SOURCE)
    defs = comp.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    df = result.output_for_node("out")
    assert list(df["id"]) == [1, 2]
    assert list(df["llm_response"]) == ["echo:Say: alpha", "echo:Say: beta"]
    assert prompts == ["Say: alpha", "Say: beta"]


def test_upstream_asset_still_works(mod, monkeypatch):
    import litellm
    monkeypatch.setattr(litellm, "completion", lambda model, messages, **kw: types.SimpleNamespace(
        choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="ok"))]))

    @dg.asset
    def raw():
        return pd.DataFrame({"body": ["x"]})

    comp = mod.LiteLLMInferenceAssetComponent(asset_name="out", prompt_template="{body}", model="gpt-4o", upstream_asset_key="raw")
    result = dg.materialize([raw, *comp.build_defs(context=None).assets])
    assert result.success and list(result.output_for_node("out")["llm_response"]) == ["ok"]


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", prompt_template="Say: {body}", model="gpt-4o")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.LiteLLMInferenceAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.LiteLLMInferenceAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
