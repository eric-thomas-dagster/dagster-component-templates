"""refresh_item_scores guarantees per backend.

  * Databricks (no multi-statement transactions): per-call staging table + ONE MERGE -> old scores or new, never partial.
  * BigQuery: a per-call staging table, so concurrent refreshes cannot interleave rows and break the MERGE.
Fakes: sqlite behind a Databricks-shaped connection (it emulates MERGE INTO as one atomic statement, which is what Delta guarantees);
BigQuery via bq_fake (sqlglot -> DuckDB). Nothing here proves behaviour on the real services."""
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import load_module  # noqa: E402
from .bq_fake import FakeBigQueryClient, install_fake_bigquery  # noqa: E402
from .test_bigquery_backend import item  # noqa: E402


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_atomic_refresh_under_test")


class Boom(RuntimeError):
    pass


class _DbxConn:
    """sqlite with databricks-sql-connector's shape (named params, <=255 markers) that can fail at the Nth statement and that emulates
    ``MERGE INTO t USING s ON t.item_id = s.item_id`` (our exact shape) atomically inside a savepoint."""
    log: list = []
    fail_at: int | None = None

    def __init__(self, path):
        self._c = sqlite3.connect(path, isolation_level=None)

    def cursor(self):
        cur, conn = self._c.cursor(), self._c

        class Cur:
            description = property(lambda self: cur.description)

            def _tick(self, sql):
                names = set(re.findall(r"(?<![:\w]):([A-Za-z_]\w*)", re.sub(r"'[^']*'", "''", sql)))
                assert len(names) <= 255
                _DbxConn.log.append(sql)
                if _DbxConn.fail_at is not None and len(_DbxConn.log) == _DbxConn.fail_at:
                    raise Boom(f"injected failure at statement {_DbxConn.fail_at}: {sql[:60]}")

            def execute(self, sql, params=None):
                self._tick(sql)
                m = re.match(r"MERGE INTO (\S+) t USING (\S+) s ON", sql)
                if m:
                    t, s = m.groups()
                    conn.execute("SAVEPOINT m")
                    conn.execute(f"UPDATE {t} SET retrievals = s.retrievals, outcome_weight = s.outcome_weight, outcome_sum = s.outcome_sum, "
                                 f"prior = s.prior, updated_at = s.updated_at FROM {s} s WHERE {t}.item_id = s.item_id")
                    conn.execute(f"INSERT INTO {t} SELECT item_id, retrievals, outcome_weight, outcome_sum, prior, updated_at FROM {s} "
                                 f"WHERE item_id NOT IN (SELECT item_id FROM {t})")
                    conn.execute("RELEASE m")
                    return cur
                return cur.execute(sql, params or {})

            def executemany(self, sql, seq):
                for p in seq:
                    self.execute(sql, p)

            def __getattr__(self, name):
                return getattr(cur, name)
        return Cur()

    def commit(self): pass
    def rollback(self): pass
    def close(self): self._c.close()


def _scores(path):
    c = sqlite3.connect(path)
    try:
        return sorted(c.execute("SELECT item_id, retrievals, outcome_sum FROM context_item_scores").fetchall())
    finally:
        c.close()


def _tables(path):
    c = sqlite3.connect(path)
    try:
        return [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    finally:
        c.close()


def _seed(mod, path, n=3):
    w = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path, isolation_level=None), dialect="sqlite", paramstyle="qmark")
    for i in range(n):
        w.record_retrieval(context_name="kb", receipt_id=f"r{i}", items=[item(f"c{i}"), item("shared", rank=1)])
        w.record_outcome(f"r{i}", signal="thumbs_up")
    w.refresh_item_scores()
    return w


def test_databricks_refresh_is_all_or_nothing_at_every_failure_point(mod, tmp_path):
    path = str(tmp_path / "w.sqlite3")
    w = _seed(mod, path)
    old = _scores(path)
    w.record_retrieval(context_name="kb", receipt_id="r9", items=[item("c9"), item("shared", rank=1)])
    w.record_outcome("r9", signal="thumbs_down")

    import shutil

    def prepared_store(p):
        _DbxConn.fail_at = None
        st = mod.SqlContextReceiptStore(connection_factory=lambda: _DbxConn(p), dialect="databricks", paramstyle="named", create_tables=False)
        st._prepare()                       # table probes are not part of the refresh's statement sequence
        _DbxConn.log = []
        return st

    # learn the statement sequence of one successful refresh on a copy, then fail at every position on fresh copies
    probe_path = str(tmp_path / "probe.sqlite3")
    shutil.copy(path, probe_path)
    prepared_store(probe_path).refresh_item_scores()
    sequence = list(_DbxConn.log)
    n = len(sequence)
    new = _scores(probe_path)
    assert new != old and n > 5
    assert any(s.startswith("MERGE INTO") for s in sequence) and not any(s.startswith("DELETE FROM `context_item_scores`") for s in sequence)
    for k in range(1, n + 1):
        p = str(tmp_path / f"f{k}.sqlite3")
        shutil.copy(path, p)
        store = prepared_store(p)
        _DbxConn.fail_at = k
        try:
            store.refresh_item_scores()
        except Boom:
            pass
        _DbxConn.fail_at = None
        assert _scores(p) in (old, new), f"half-written scores after failure at statement {k}: {_scores(p)}"
        if not sequence[k - 1].startswith("DROP"):       # a failing DROP is the one best-effort step; any other failure still cleans up
            assert not [t for t in _tables(p) if "_stg_" in t], f"staging table leaked after failure at statement {k}"


def test_databricks_refresh_chunks_staging_inserts_under_the_marker_cap(mod, tmp_path):
    path = str(tmp_path / "w.sqlite3")
    w = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(path, isolation_level=None), dialect="sqlite", paramstyle="qmark")
    n = 700
    w.record_retrieval(context_name="kb", receipt_id="big", items=[item(f"c{i:04d}", rank=i) for i in range(n)])
    w.record_outcome("big", signal="thumbs_up")
    _DbxConn.log, _DbxConn.fail_at = [], None
    store = mod.SqlContextReceiptStore(connection_factory=lambda: _DbxConn(path), dialect="databricks", paramstyle="named", create_tables=False)
    assert store.refresh_item_scores() == n and len(_scores(path)) == n          # _DbxConn asserts <= 255 markers per statement


# ── BigQuery ──────────────────────────────────────────────────────────────────────────────────────────

def test_bigquery_concurrent_refreshes_do_not_share_a_staging_table(mod, monkeypatch):
    client = FakeBigQueryClient("proj")
    client.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(client))
    a = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")
    b = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")
    for i in range(3):
        a.record_retrieval(context_name="kb", receipt_id=f"r{i}", items=[item(f"c{i}")])
        a.record_outcome(f"r{i}", signal="thumbs_up")
    real_query, fired = a._query, []

    def interleaved(sql, **kw):
        out = real_query(sql, **kw)
        if sql.lstrip().upper().startswith("CREATE TABLE") and not fired:
            fired.append(1)
            b.refresh_item_scores()          # a whole second refresh lands between A's staging create and A's load/merge
        return out
    a._query = interleaved
    assert a.refresh_item_scores() == 3 and fired
    assert sorted(a.get_item_scores(["c0", "c1", "c2"])) == ["c0", "c1", "c2"]
    left = client.con.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%staging%'").fetchall()
    assert left == []                          # every per-call staging table was dropped
