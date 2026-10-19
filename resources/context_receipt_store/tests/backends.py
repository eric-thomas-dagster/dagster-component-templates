"""Shared helpers for the selftest / privacy / maintenance tests: build each backend the same way and dump every stored cell.
BigQuery is the DuckDB-backed fake (bq_fake.py); sqlite and duckdb SQL stores are real."""
import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .bq_fake import FakeBigQueryClient, install_fake_bigquery  # noqa: E402

# dbapi-*: the same SqlContextReceiptStore on a plain PEP-249 connection factory (no SQLAlchemy): real sqlite3 (paramstyle qmark)
# and real duckdb (qmark), so every parametrized suite also proves the DBAPI executor.
KINDS = ["sqlite", "sql-sqlite", "sql-duckdb", "dbapi-sqlite", "dbapi-duckdb", "bigquery-fake"]
DBAPI_KINDS = ["dbapi-sqlite", "dbapi-duckdb"]


def load_module(name="context_receipt_store_new_features_under_test"):
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_store(kind, mod, tmp_path, monkeypatch, *, async_writes=False):
    if kind == "sqlite":
        return mod.ContextReceiptStore(str(tmp_path / "r.sqlite3"))
    if kind == "bigquery-fake":
        client = FakeBigQueryClient("proj")
        client.create_dataset("proj.ds")
        monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(client))
        cls = mod.AsyncBigQueryContextReceiptStore if async_writes else mod.BigQueryContextReceiptStore
        return cls("proj.ds", client_factory=lambda: client, table_prefix="t_")
    cls = mod.AsyncSqlContextReceiptStore if async_writes else mod.SqlContextReceiptStore
    if kind == "dbapi-sqlite":
        path = str(tmp_path / "d.sqlite3")
        return cls(connection_factory=lambda: sqlite3.connect(path), dialect="sqlite", table_prefix="g_")
    if kind == "dbapi-duckdb":
        duckdb = pytest.importorskip("duckdb")
        path = str(tmp_path / "d.duckdb")
        return cls(connection_factory=lambda: duckdb.connect(path), dialect="duckdb", table_prefix="g_")
    sa = pytest.importorskip("sqlalchemy")
    if kind == "sql-duckdb":
        pytest.importorskip("duckdb_engine")
        engine = sa.create_engine(f"duckdb:///{tmp_path / 'r.duckdb'}")
    else:
        engine = sa.create_engine(f"sqlite:///{tmp_path / 'g.sqlite3'}")
    cls = mod.AsyncSqlContextReceiptStore if async_writes else mod.SqlContextReceiptStore
    return cls(engine, table_prefix="g_")


def all_cells(store):
    """Every stored cell of every receipt table, as strings (to prove a raw id is nowhere)."""
    cells = []
    if hasattr(store, "_query"):
        for name in ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events", "context_item_scores"):
            for row in store._query(f"SELECT * FROM {store._t(name)}"):
                cells += [str(v) for v in row.values()]
    else:
        conn = sqlite3.connect(store.database_path)
        for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall():
            for row in conn.execute(f'SELECT * FROM "{table}"').fetchall():
                cells += [str(v) for v in row]
        conn.close()
    return cells


def row_counts(store):
    out = {}
    for name in ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events"):
        if hasattr(store, "_query"):
            out[name] = store._query(f"SELECT COUNT(1) AS n FROM {store._t(name)}")[0]["n"]
        else:
            conn = sqlite3.connect(store.database_path)
            out[name] = conn.execute(f"SELECT COUNT(1) FROM {name}").fetchone()[0]
            conn.close()
    return out
