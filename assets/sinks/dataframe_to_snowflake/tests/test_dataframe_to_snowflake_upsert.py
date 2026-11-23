"""Tests for DataframeToSnowflakeComponent's if_exists='upsert' (ADF SQL sink parity, ask
11): write to a staging table, then MERGE into the target by upsert_key_columns. Only the
Snowflake wire boundary is faked (connect / write_pandas / cursor.execute+fetchone).
"""
import dagster as dg
import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.pandas_tools

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._last_sql = None

    def execute(self, sql, params=None):
        self.conn.executed.append(sql)
        self._last_sql = sql

    def fetchone(self):
        if "INFORMATION_SCHEMA.TABLES" in self._last_sql:
            return (1 if self.conn.target_exists else 0,)
        return None

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs, target_exists):
        self.kwargs = kwargs
        self.executed = []
        self.target_exists = target_exists

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass


@pytest.fixture()
def sf(monkeypatch):
    state = {"conns": [], "writes": [], "target_exists": False}

    def fake_connect(**kwargs):
        c = FakeConn(kwargs, state["target_exists"])
        state["conns"].append(c)
        return c

    def fake_write_pandas(conn, df, table_name, **kwargs):
        state["writes"].append((df.copy(), table_name))
        return True, 1, len(df), []

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setattr(snowflake.connector.pandas_tools, "write_pandas", fake_write_pandas)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return state


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2], "amount": [10, 20]})
    return orders_flat


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_sf")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table", "out")
    kw.setdefault("if_exists", "upsert")
    return mod.DataframeToSnowflakeComponent(**kw)


# ---------------------------------------------------------------------------
# build_merge_sql -- pure function
# ---------------------------------------------------------------------------


def test_build_merge_sql_shape(mod):
    sql = mod.build_merge_sql(None, "T", "T_STAGE", ["ID", "AMOUNT"], ["ID"])
    assert sql.startswith('MERGE INTO "T" AS t USING "T_STAGE" AS s ON t."ID" = s."ID" ')
    assert 'WHEN MATCHED THEN UPDATE SET "AMOUNT" = s."AMOUNT"' in sql
    assert 'WHEN NOT MATCHED THEN INSERT ("ID", "AMOUNT") VALUES (s."ID", s."AMOUNT")' in sql


def test_build_merge_sql_no_non_key_columns_omits_update_clause(mod):
    sql = mod.build_merge_sql(None, "T", "T_STAGE", ["ID"], ["ID"])
    assert "WHEN MATCHED" not in sql


# ---------------------------------------------------------------------------
# Component: upsert path
# ---------------------------------------------------------------------------


def test_upsert_requires_key_columns(mod, sf):
    comp = _comp(mod)  # no upsert_key_columns
    defs = comp.build_defs(load_context=None)
    with pytest.raises(ValueError, match="upsert_key_columns"):
        dg.materialize([_flat_upstream(), *defs.assets])


def test_upsert_against_existing_table_runs_merge(mod, sf):
    sf["target_exists"] = True
    comp = _comp(mod, upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    conn = sf["conns"][0]
    merge_statements = [s for s in conn.executed if s.startswith("MERGE INTO")]
    assert len(merge_statements) == 1
    assert 'ON t."ID" = s."ID"' in merge_statements[0]
    assert any(s.startswith("DROP TABLE IF EXISTS") for s in conn.executed)


def test_upsert_against_missing_table_renames_staging(mod, sf):
    sf["target_exists"] = False
    comp = _comp(mod, upsert_key_columns=["id"])
    defs = comp.build_defs(load_context=None)
    result = dg.materialize([_flat_upstream(), *defs.assets])
    assert result.success
    conn = sf["conns"][0]
    assert any("RENAME TO" in s for s in conn.executed)
    assert not any(s.startswith("MERGE INTO") for s in conn.executed)
