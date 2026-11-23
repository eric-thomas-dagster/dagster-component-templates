"""Tests for DataframeToDatabricksComponent's mode='upsert' (ADF SQL sink parity, ask 11):
write to a staging table, then MERGE into the target by upsert_key_columns (Databricks
SQL/Delta's native upsert mechanism). Only the databricks-sql-connector wire boundary is
faked (a minimal injected `databricks.sql` module, matching this suite's own convention).
"""
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .test_dataframe_to_databricks import _comp, _flat_upstream  # noqa: F401 -- reuse helpers


@pytest.fixture()
def mod():
    from .conftest import load_component_module
    return load_component_module()


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *a):
        self.conn.statements.append(sql)

    def executemany(self, sql, rows):
        self.conn.statements.append(sql)
        self.conn.rows.extend(rows)

    def fetchone(self):
        return (1 if self.conn.target_exists else 0,)


class FakeConn:
    def __init__(self, target_exists):
        self.statements = []
        self.rows = []
        self.target_exists = target_exists

    def cursor(self):
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture()
def dbx(monkeypatch):
    state = {"conns": [], "target_exists": False}
    databricks = types.ModuleType("databricks")
    databricks.__path__ = []
    sql = types.ModuleType("databricks.sql")

    def connect(**kwargs):
        c = FakeConn(state["target_exists"])
        state["conns"].append(c)
        return c

    sql.connect = connect
    databricks.sql = sql
    monkeypatch.setitem(sys.modules, "databricks", databricks)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql)
    monkeypatch.setenv("DATABRICKS_HOST", "env-host.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    return state


def test_upsert_requires_key_columns(mod, dbx):
    comp = _comp(mod, mode="upsert")
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="upsert_key_columns"):
        dg.materialize([_flat_upstream(), *defs.assets])


def test_upsert_against_missing_table_renames_staging(mod, dbx):
    dbx["target_exists"] = False
    comp = _comp(mod, mode="upsert", upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    conn = dbx["conns"][0]
    assert any("RENAME TO" in s for s in conn.statements)
    assert not any(s.startswith("MERGE INTO") for s in conn.statements)


def test_upsert_against_existing_table_runs_merge(mod, dbx):
    dbx["target_exists"] = True
    comp = _comp(mod, mode="upsert", upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    conn = dbx["conns"][0]
    merges = [s for s in conn.statements if s.startswith("MERGE INTO")]
    assert len(merges) == 1
    assert "ON t.`id` = s.`id`" in merges[0]
    assert any(s.startswith("DROP TABLE IF EXISTS") for s in conn.statements)
