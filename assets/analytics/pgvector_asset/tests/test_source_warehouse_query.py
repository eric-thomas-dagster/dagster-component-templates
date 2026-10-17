"""Tests for PgvectorAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the litellm embedding call and the Postgres target engine (the source read is real sqlite) is mocked."""
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

def test_source_warehouse_query_embeds_and_writes(mod, sqlite_url, monkeypatch):
    import litellm
    import sqlalchemy
    monkeypatch.setenv("PG_URL", "postgresql://fake/fake")
    monkeypatch.setattr(litellm, "embedding", lambda input, **kw: types.SimpleNamespace(
        data=[{"embedding": [float(len(t)), 0.0, 1.0]} for t in input]))
    inserts = []

    class FakeConn:
        def execute(self, stmt, params=None):
            if params:
                inserts.append(params)
    class FakeBegin:
        def __enter__(self):
            return FakeConn()
        def __exit__(self, *a):
            return False
    class FakeEngine:
        def begin(self):
            return FakeBegin()

    real_create = sqlalchemy.create_engine
    monkeypatch.setattr(sqlalchemy, "create_engine",
                        lambda url, *a, **kw: real_create(url, *a, **kw) if str(url).startswith("sqlite") else FakeEngine())
    comp = mod.PgvectorAssetComponent(asset_name="out", id_column="id", text_column="body", database_url_env_var="PG_URL",
                                      target_table="t", dimensions=3, source=SOURCE)
    result = dg.materialize(list(comp.build_defs(context=None).assets))
    assert result.success
    assert sorted(p["id"] for p in inserts) == ["1", "2"]
    assert sorted(p["text"] for p in inserts) == ["alpha", "beta"]


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", id_column="id", text_column="body", database_url_env_var="PG_URL", target_table="t")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.PgvectorAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.PgvectorAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
