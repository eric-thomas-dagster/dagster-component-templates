"""Tests for WandbAssetComponent's dual dataframe-or-SQL input
(source: {kind: warehouse_query, ...}), against a real sqlite database.
Only the wandb client (wandb is not installed; a fake module records run.log calls) is mocked."""
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

def test_source_warehouse_query_logs_sql_rows(mod, sqlite_url, monkeypatch):
    logged = []
    fake = types.ModuleType("wandb")
    fake.login = lambda key=None, **kw: True
    run = types.SimpleNamespace(id="run1", log=lambda metrics, step=None: logged.append((metrics, step)),
                                finish=lambda *a, **k: None, summary={}, get_url=lambda: "http://w")
    fake.init = lambda **kw: run
    monkeypatch.setitem(sys.modules, "wandb", fake)
    monkeypatch.setenv("WANDB_API_KEY", "k")
    comp = mod.WandbAssetComponent(asset_name="out", project="p", mode="log_metrics", step_column="id",
                                   metrics_columns=["score"], source=SOURCE)
    result = dg.materialize(list(comp.build_defs(context=None).assets))
    assert result.success
    assert logged == [({"score": 0.5}, 1), ({"score": 1.5}, 2)]


def test_source_rejected_outside_log_metrics_mode(mod):
    comp = mod.WandbAssetComponent(asset_name="out", project="p", mode="log_artifact", source=SOURCE)
    with pytest.raises(ValueError, match="only supported in"):
        comp.build_defs(context=None)


def test_upstream_asset_key_and_source_mutually_exclusive(mod):
    base = dict(asset_name="out", project="p", mode="log_metrics")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.WandbAssetComponent(**base).build_defs(context=None)
    with pytest.raises(ValueError, match="set exactly one"):
        mod.WandbAssetComponent(**base, upstream_asset_key="some_asset", source=SOURCE).build_defs(context=None)
