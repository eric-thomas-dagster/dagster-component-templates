"""Core search behavior for TavilyIngestionComponent: multi-query search, the
upstream_asset_key/query_column dual-mode queries path, and the missing-API-key error.

`dlt.pipeline` is faked (see conftest.py, `consume=True`) so no real DuckDB file is
touched, while still eagerly iterating the resource generator (while the run is still
live) to exercise the real Tavily request-building / row-shaping logic against a faked
`requests.post` -- the real HTTP call to Tavily is never made.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_tavily_search, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _build(mod, **overrides):
    kwargs = dict(asset_name="test_asset", queries="dagster, snowflake")
    kwargs.update(overrides)
    return mod.TavilyIngestionComponent(**kwargs)


def _two_result_responder(query, headers=None, payload=None):
    q = payload["query"] if payload else query
    return {
        "query": q,
        "results": [
            {"title": f"{q} result 1", "url": "https://a.example", "content": "snippet one", "score": 0.91, "id": "r1"},
            {"title": f"{q} result 2", "url": "https://b.example", "content": "snippet two", "score": 0.42, "id": "r2"},
        ],
    }


def test_multi_query_search_lands_one_row_per_result(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    seen_headers = []

    def responder(url, headers, payload):
        seen_headers.append(headers)
        assert url == mod.TAVILY_SEARCH_URL
        return _two_result_responder(None, headers, payload)

    install_fake_tavily_search(monkeypatch, responder)
    fake_pipeline = install_fake_dlt(monkeypatch, mod, consume=True)

    component = _build(mod, queries="dagster, snowflake")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])

    rows = fake_pipeline.captured_rows

    assert len(rows) == 4  # 2 queries x 2 results each
    assert {r["query"] for r in rows} == {"dagster", "snowflake"}
    for r in rows:
        assert {"query", "title", "url", "content", "score", "result_id"} <= set(r)
        assert "raw_content" not in r
        assert "answer" not in r
    assert all(h["Authorization"] == "Bearer test-key" for h in seen_headers)


def test_emits_the_document_source_contract_columns(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    install_fake_tavily_search(monkeypatch, lambda url, headers, payload: _two_result_responder(None, headers, payload))
    fake_pipeline = install_fake_dlt(monkeypatch, mod, consume=True)

    component = _build(mod, queries="dagster")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])

    rows = fake_pipeline.captured_rows
    assert len(rows) == 2
    for r in rows:
        assert r["doc_id"] == r["result_id"]
        assert r["content"] in ("snippet one", "snippet two")
        assert r["source_type"] == "tavily"
        assert r["updated_at"]
        assert r["content_hash"]


def test_include_answer_duplicates_answer_onto_every_row_for_that_query(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")

    def responder(url, headers, payload):
        out = _two_result_responder(None, headers, payload)
        out["answer"] = f"a short answer about {payload['query']}"
        return out

    install_fake_tavily_search(monkeypatch, responder)
    fake_pipeline = install_fake_dlt(monkeypatch, mod, consume=True)

    component = _build(mod, queries="dagster", include_answer="basic")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])

    rows = fake_pipeline.captured_rows
    assert len(rows) == 2
    assert all(r["answer"] == "a short answer about dagster" for r in rows)


def test_include_raw_content_adds_raw_content_column(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")

    def responder(url, headers, payload):
        out = _two_result_responder(None, headers, payload)
        for r in out["results"]:
            r["raw_content"] = f"full page text for {r['title']}"
        return out

    install_fake_tavily_search(monkeypatch, responder)
    fake_pipeline = install_fake_dlt(monkeypatch, mod, consume=True)

    component = _build(mod, queries="dagster", include_raw_content="markdown")
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def])

    rows = fake_pipeline.captured_rows
    assert all("raw_content" in r and r["raw_content"] for r in rows)


def test_missing_queries_and_upstream_asset_key_raises_at_build_defs(mod):
    component = mod.TavilyIngestionComponent(asset_name="test_asset")
    with pytest.raises(ValueError, match="queries"):
        component.build_defs(context=None)


def test_missing_api_key_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("TAVILY_API_KEY", raising=False)
    install_fake_dlt(monkeypatch, mod)

    component = _build(mod)
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="TAVILY_API_KEY"):
        dg.materialize([asset_def])


def test_upstream_asset_key_queries_are_combined_with_static_queries(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")

    def responder(url, headers, payload):
        return _two_result_responder(None, headers, payload)

    install_fake_tavily_search(monkeypatch, responder)
    fake_pipeline = install_fake_dlt(monkeypatch, mod, consume=True)

    @dg.asset(key=dg.AssetKey("search_terms"))
    def search_terms():
        return pd.DataFrame({"q": ["upstream query one", "upstream query two"]})

    component = _build(
        mod, queries="dagster", upstream_asset_key="search_terms", query_column="q"
    )
    tavily_asset = component.build_defs(context=None).assets[0]
    dg.materialize([search_terms, tavily_asset])

    rows = fake_pipeline.captured_rows
    queries_seen = {r["query"] for r in rows}
    assert queries_seen == {"dagster", "upstream query one", "upstream query two"}


def test_upstream_asset_key_missing_query_column_raises(mod, monkeypatch):
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")
    install_fake_dlt(monkeypatch, mod)

    @dg.asset(key=dg.AssetKey("search_terms"))
    def search_terms():
        return pd.DataFrame({"not_q": ["x"]})

    component = _build(mod, queries=None, upstream_asset_key="search_terms", query_column="q")
    tavily_asset = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="query_column"):
        dg.materialize([search_terms, tavily_asset])
