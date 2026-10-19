"""Fixes from the official-documentation audit of the warehouse code paths (nobody can run the warehouses; the docs are the check):

  - BigQuery load jobs must not relax REQUIRED columns to NULLABLE (a load job's explicit NULLABLE schema onto our NOT NULL tables fails)
  - BigQuery query parameters cannot be NULL
  - databricks-sql-connector's native parameters cap at 255 markers per query, and LIMIT takes an inline integer, never a marker
"""
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import load_module  # noqa: E402
from bq_fake import FakeBigQueryClient, install_fake_bigquery  # noqa: E402
from test_bigquery_backend import item  # noqa: E402


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_doc_audit_under_test")


@pytest.fixture()
def client(monkeypatch):
    c = FakeBigQueryClient("proj")
    c.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(c))
    return c


# ── B1 ───────────────────────────────────────────────────────────────────────────────────────────────

def test_fake_rejects_a_nullable_schema_load_onto_a_required_column(client):
    """The fake itself behaves like BigQuery: relaxing REQUIRED -> NULLABLE through a load job's schema fails (guards the regression test below)."""
    bq = sys.modules["google.cloud.bigquery"]
    client.con.execute("CREATE TABLE ds.t_req (a VARCHAR NOT NULL, b VARCHAR)")
    cfg = bq.LoadJobConfig(write_disposition="WRITE_APPEND", schema=[bq.SchemaField("a", "STRING"), bq.SchemaField("b", "STRING")])
    with pytest.raises(RuntimeError, match="REQUIRED to NULLABLE"):
        client.load_table_from_json([{"a": "x", "b": "y"}], "proj.ds.t_req", job_config=cfg)
    client.load_table_from_json([{"a": "x", "b": "y"}], "proj.ds.t_req", job_config=bq.LoadJobConfig(write_disposition="WRITE_APPEND"))
    assert client.con.execute("SELECT COUNT(*) FROM ds.t_req").fetchone()[0] == 1


def test_receipt_writes_do_not_send_a_schema_that_relaxes_required_columns(mod, client):
    store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")
    rid = store.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a")], caller_id="u1")
    store.record_outcome("r1", signal="thumbs", value=1.0)
    assert rid == "r1" and store.get_receipt("r1")["receipt_id"] == "r1"
    # the DDL really made NOT NULL columns, so the check above is meaningful
    assert client.con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name LIKE 't_context_receipt%' AND is_nullable = 'NO'"
                              ).fetchone()[0] > 0


def test_only_the_staging_load_sends_an_explicit_schema(mod, client):
    store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")
    seen = []
    real = client.load_table_from_json

    def spy(rows, destination, job_config=None, **kw):
        seen.append((str(destination), getattr(job_config, "schema", None)))
        return real(rows, destination, job_config=job_config, **kw)
    client.load_table_from_json = spy
    store.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a")], excluded=[{"item_id": "z", "reason": "pii", "source_asset_key": "kb"}])
    store.record_outcome("r1", signal="thumbs", value=1.0)
    store.refresh_item_scores()
    assert seen
    for dest, schema in seen:
        assert (schema is not None) == ("context_item_scores_staging" in dest), (dest, schema)


# ── B7 ───────────────────────────────────────────────────────────────────────────────────────────────

def test_none_query_parameter_is_a_clear_error(mod, client):
    bq = sys.modules["google.cloud.bigquery"]
    with pytest.raises(ValueError, match="cannot be NULL"):
        mod.BigQueryContextReceiptStore._params(bq, {"a": None})
    assert [p.name for p in mod.BigQueryContextReceiptStore._params(bq, {"a": "x", "b": 1})] == ["a", "b"]


# ── D1 ───────────────────────────────────────────────────────────────────────────────────────────────

class _DatabricksShapedConnection:
    """sqlite behind a databricks-sql-connector-shaped connection: paramstyle named, <= 255 markers per statement, no marker after LIMIT."""
    seen: list = []

    def __init__(self, path):
        self._c = sqlite3.connect(path, isolation_level=None)

    def cursor(self):
        cur, outer = self._c.cursor(), self

        class Cur:
            description = property(lambda self: cur.description)

            def execute(self, sql, params=None):
                names = set(re.findall(r"(?<![:\w]):([A-Za-z_]\w*)", re.sub(r"'[^']*'", "''", sql)))
                assert len(names) <= 255, f"{len(names)} markers: databricks-sql-connector rejects more than 255"
                assert not re.search(r"LIMIT\s+:", sql, re.I), f"marker after LIMIT (needs a foldable integer): {sql}"
                outer.seen.append(sql)
                return cur.execute(sql, params or {})

            def __getattr__(self, name):
                return getattr(cur, name)
        return Cur()

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self._c.close()


def test_databricks_in_lists_are_chunked_under_255_markers_and_limit_is_inline(mod, tmp_path):
    path = str(tmp_path / "w.sqlite3")
    writer = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path, isolation_level=None), dialect="sqlite", paramstyle="qmark")
    n = 600
    for i in range(n):
        writer.record_retrieval(context_name="kb", receipt_id=f"r{i:04d}", items=[item("a")])
    writer.close()
    _DatabricksShapedConnection.seen = []
    dbx = mod.SqlContextReceiptStore(connection_factory=lambda: _DatabricksShapedConnection(path), dialect="databricks", paramstyle="named",
                                     create_tables=False)
    out = dbx.read_since(limit=1000)
    assert len(out["receipts"]) == n and all(len(r["items"]) == 1 for r in out["receipts"])          # every chunk of ids was read; none lost
    in_queries = [q for q in _DatabricksShapedConnection.seen if "receipt_id IN" in q]
    assert len(in_queries) >= 2 * 3                                     # 600 ids / 200 per chunk, items + excluded
    assert any("LIMIT 1000" in q for q in _DatabricksShapedConnection.seen)
    assert list(dbx._in_chunks(list(range(450)))) == [list(range(200)), list(range(200, 400)), list(range(400, 450))]


def test_other_dialects_keep_500_per_chunk(mod, tmp_path):
    store = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(str(tmp_path / "x.sqlite3")), dialect="sqlite", paramstyle="qmark")
    assert [len(c) for c in store._in_chunks(list(range(1200)))] == [500, 500, 200]
