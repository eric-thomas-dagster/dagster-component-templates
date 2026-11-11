"""Destination wiring for TavilyIngestionComponent: default in-memory DuckDB ->
DataFrame read-back, persist_only -> MaterializeResult, and the non-SQL-destination
warning path. `dlt.pipeline` is faked (see conftest.py); the real Tavily API is mocked
too since these tests do materialize the asset end-to-end.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, install_fake_tavily_search, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _fake_search(monkeypatch):
    def responder(url, headers, payload):
        return {
            "query": payload["query"],
            "results": [
                {"title": "t", "url": "https://a.example", "content": "c", "score": 0.5, "id": "r1"},
            ],
        }

    install_fake_tavily_search(monkeypatch, responder)
    monkeypatch.setenv("TAVILY_API_KEY", "test-key")


def test_default_duckdb_destination_returns_dataframe(mod, monkeypatch):
    readback = pd.DataFrame({"query": ["dagster"], "title": ["t"], "content": ["c"]})
    install_fake_dlt(monkeypatch, mod, readback_df=readback)

    component = mod.TavilyIngestionComponent(asset_name="test_asset", queries="dagster")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success

    df = result.output_for_node("test_asset")
    assert len(df) == 1
    assert list(df["title"]) == ["t"]


def test_persist_only_with_destination_emits_materialize_result(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)

    component = mod.TavilyIngestionComponent(
        asset_name="test_asset", queries="dagster", destination="snowflake", persist_only=True
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success


def test_non_sql_destination_emits_materialize_result_without_persist_only(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)

    component = mod.TavilyIngestionComponent(
        asset_name="test_asset", queries="dagster", destination="filesystem", bucket_url="file:///tmp/x"
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
