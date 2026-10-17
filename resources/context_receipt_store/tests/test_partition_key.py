"""Receipt-level `dagster_partition_key` (the PRODUCING asset's partition) on every backend: write, round-trip through get_receipt /
read_since / export_events / ingest, `receipts_for_partition`, async wrappers, and the additive upgrade of tables made by an older release.
SQLite and DuckDB are real; BigQuery is the DuckDB-backed fake (see bq_fake.py)."""
import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import KINDS, load_module, make_store  # noqa: E402

SCHEMA = json.loads((Path(__file__).parents[1] / "receipt-spec.v1.json").read_text())


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_partition_key_under_test")


def _item(i, partition=None):
    return {"item_id": i, "source_asset_key": "docs", "source_version": "v1", "source_partition": partition, "rank": 0, "score": 0.5}


def _write(store):
    store.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a", "src-1")], dagster_asset_key="rag", dagster_partition_key="2024-01-01")
    store.record_retrieval(context_name="rag", receipt_id="r2", items=[_item("a", "src-2")], dagster_asset_key="rag", dagster_partition_key="2024-01-02")
    store.record_retrieval(context_name="rag", receipt_id="r3", items=[_item("a")], dagster_asset_key="rag", dagster_partition_key="2024-01-01|eu")
    store.record_retrieval(context_name="rag", receipt_id="r4", items=[_item("a")], dagster_asset_key="rag")                       # unpartitioned
    store.record_retrieval(context_name="rag", receipt_id="r5", items=[_item("a")], dagster_asset_key="other", dagster_partition_key="2024-01-01")


@pytest.mark.parametrize("kind", KINDS)
def test_round_trip_and_receipts_for_partition_parity(kind, mod, tmp_path, monkeypatch):
    store = make_store(kind, mod, tmp_path, monkeypatch)
    _write(store)
    assert store.get_receipt("r1")["dagster_partition_key"] == "2024-01-01"
    assert store.get_receipt("r3")["dagster_partition_key"] == "2024-01-01|eu"
    assert store.get_receipt("r4")["dagster_partition_key"] is None
    assert store.receipts_for_partition("rag", "2024-01-01") == ["r1"]          # exact key; other asset and multi-key excluded
    assert store.receipts_for_partition("rag", "2024-01-01|eu") == ["r3"]
    assert store.receipts_for_partition("rag", "2024-01-02") == ["r2"]
    assert store.receipts_for_partition("rag", None) == ["r4"]
    assert store.receipts_for_partition("rag", "nope") == []
    assert store.receipts_for_partition("other", "2024-01-01") == ["r5"]
    # the source-partition lookup is unaffected: it keys on items, not on the receipt's own partition
    assert store.find_by_assets({"docs": {"src-1"}})["receipt_ids"] == ["r1"]
    tail = {r["receipt_id"]: r["dagster_partition_key"] for r in store.read_since(0, 0, 100)["receipts"]}
    assert tail == {"r1": "2024-01-01", "r2": "2024-01-02", "r3": "2024-01-01|eu", "r4": None, "r5": "2024-01-01"}
    assert store.export_events("r3")[0]["dagster_partition_key"] == "2024-01-01|eu"


@pytest.mark.parametrize("kind", KINDS)
def test_ingest_round_trips_and_conforms_to_spec(kind, mod, tmp_path, monkeypatch):
    jsonschema = pytest.importorskip("jsonschema")
    src = mod.ContextReceiptStore(str(tmp_path / "src.db"))
    src.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")], dagster_partition_key="2024-01-01|eu")
    event = src.export_events("r1")[0]
    jsonschema.Draft202012Validator(SCHEMA).validate(event)
    dst = make_store(kind, mod, tmp_path, monkeypatch)
    dst.ingest(event)
    assert dst.get_receipt("r1")["dagster_partition_key"] == "2024-01-01|eu"
    with pytest.raises(Exception):   # the spec still rejects unknown fields
        jsonschema.Draft202012Validator(SCHEMA).validate({**event, "partition": "x"})


@pytest.mark.parametrize("kind", [k for k in KINDS if k != "sqlite"])
def test_async_wrappers_pass_the_key_through(kind, mod, tmp_path, monkeypatch):
    store = make_store(kind, mod, tmp_path, monkeypatch, async_writes=True)
    store.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")], dagster_asset_key="rag", dagster_partition_key="p1")
    store.ingest({"spec_version": "1", "type": "retrieval", "receipt_id": "r2", "context_name": "rag", "dagster_asset_key": "rag",
                  "dagster_partition_key": "p2", "items": [_item("a")]})
    store.flush()
    assert not store.dead_letters
    assert store.receipts_for_partition("rag", "p1") == ["r1"] and store.receipts_for_partition("rag", "p2") == ["r2"]


def _old_ddl(mod, dataset, prefix, dialect):
    """The receipts DDL exactly as the previous release shipped it: the current DDL without the new column."""
    ddl = mod.receipt_table_ddl(dataset, prefix, dialect)
    old = [re.sub(r",\s*dagster_partition_key \w+(\(\d+\))?\)", ")", d) for d in ddl]
    assert old[0] != ddl[0] and "dagster_partition_key" not in old[0]
    return old


@pytest.mark.parametrize("kind", KINDS)
def test_older_tables_are_upgraded_additively(kind, mod, tmp_path, monkeypatch):
    store = make_store(kind, mod, tmp_path, monkeypatch)
    if kind == "sqlite":
        con = sqlite3.connect(store.database_path)
        con.executescript("""CREATE TABLE context_receipts (receipt_id TEXT PRIMARY KEY, created_at TEXT NOT NULL, context_name TEXT NOT NULL,
            context_version TEXT, policy_version TEXT, caller_id TEXT, purpose TEXT, query_hash TEXT, eligible_count INTEGER, excluded_count INTEGER,
            dagster_asset_key TEXT, dagster_run_id TEXT, trace_id TEXT, mcp_session_id TEXT, mcp_request_id TEXT);
            INSERT INTO context_receipts (receipt_id, created_at, context_name, dagster_asset_key) VALUES ('old', '2020-01-01T00:00:00+00:00', 'rag', 'rag');""")
        con.commit(); con.close()
    elif kind == "bigquery-fake":
        client = store._client_factory()
        for ddl in _old_ddl(mod, store.dataset, store.table_prefix, "bigquery"):
            client.query(ddl).result()
        client.query(f"INSERT INTO {store._t('context_receipts')} (receipt_id, created_at, context_name, dagster_asset_key) "
                     f"VALUES ('old', TIMESTAMP '2020-01-01 00:00:00+00', 'rag', 'rag')").result()
    else:
        with store._exec.transaction() as conn:       # SQLAlchemy or DBAPI executor
            for ddl in _old_ddl(mod, store.schema, store.table_prefix, store.dialect):
                conn.execute(ddl)
            conn.execute(f"INSERT INTO {store._t('context_receipts')} (receipt_id, created_at, context_name, dagster_asset_key) "
                         f"VALUES ('old', '2020-01-01T00:00:00+00:00', 'rag', 'rag')")
    store.record_retrieval(context_name="rag", receipt_id="new", items=[_item("a")], dagster_asset_key="rag", dagster_partition_key="2024-01-01")
    assert store.get_receipt("old")["dagster_partition_key"] is None        # the old row survived, reads NULL
    assert store.get_receipt("new")["dagster_partition_key"] == "2024-01-01"
    assert store.receipts_for_partition("rag", "2024-01-01") == ["new"]
    assert store.receipts_for_partition("rag", None) == ["old"]
    ids = [r["receipt_id"] for r in store.read_since(0, 0, 100)["receipts"]]
    assert sorted(ids) == ["new", "old"]
    store.ensure_tables()   # idempotent: a second run finds the column and does not ALTER
    if hasattr(store, "_ready"):
        store._ready = False
        store.ensure_tables()


def test_upgrade_statements_cover_every_dialect(mod):
    sqlglot = pytest.importorskip("sqlglot")
    dialects = {"bigquery": "bigquery", "snowflake": "snowflake", "databricks": "databricks", "postgres": "postgres",
                "mysql": "mysql", "duckdb": "duckdb", "sqlite": "sqlite"}
    for d, g in dialects.items():
        (probe, alter), = mod.receipt_column_upgrades("ds" if d in ("postgres", "duckdb", "mysql") else "a.b", "p_", d)
        for sql in (probe, alter):
            sqlglot.parse_one(sql, read=g)
        assert "ADD COLUMN dagster_partition_key" in alter
        assert "dagster_partition_key" in mod.receipt_table_ddl("a.b" if d not in ("postgres", "duckdb", "mysql") else "a", "p_", d)[0]
