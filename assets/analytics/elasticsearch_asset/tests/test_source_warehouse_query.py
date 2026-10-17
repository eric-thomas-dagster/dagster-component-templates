"""Tests for ElasticsearchAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the Elasticsearch client + bulk helper (elasticsearch is not installed; fake modules are injected) is mocked."""
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

def _fake_es(monkeypatch, indexed):
    es_mod = types.ModuleType("elasticsearch")
    es_mod.Elasticsearch = lambda **kw: object()
    helpers = types.ModuleType("elasticsearch.helpers")

    def bulk(es, actions, chunk_size=500, raise_on_error=True):
        acts = list(actions)
        indexed.extend(acts)
        return len(acts), []

    helpers.bulk = bulk
    es_mod.helpers = helpers
    monkeypatch.setitem(sys.modules, "elasticsearch", es_mod)
    monkeypatch.setitem(sys.modules, "elasticsearch.helpers", helpers)
    monkeypatch.setenv("ES_URL", "http://localhost:9200")


def test_source_warehouse_query_bulk_indexes_sql_rows(mod, sqlite_url, monkeypatch):
    indexed = []
    _fake_es(monkeypatch, indexed)
    comp = mod.ElasticsearchAssetComponent(asset_name="out", elasticsearch_url_env_var="ES_URL", index_name="idx",
                                           id_field="id", source=SOURCE)
    result = dg.materialize(list(comp.build_defs(context=None).assets))
    assert result.success
    assert [a["_id"] for a in indexed] == ["1", "2"]
    assert [a["_source"]["body"] for a in indexed] == ["alpha", "beta"]
    assert all(a["_index"] == "idx" for a in indexed)


def test_source_rejected_outside_index_mode(mod):
    comp = mod.ElasticsearchAssetComponent(asset_name="out", elasticsearch_url_env_var="ES_URL", index_name="idx",
                                           mode="query", source=SOURCE)
    with pytest.raises(ValueError, match="only supported in"):
        comp.build_defs(context=None)


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", elasticsearch_url_env_var="ES_URL", index_name="idx")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ElasticsearchAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ElasticsearchAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
