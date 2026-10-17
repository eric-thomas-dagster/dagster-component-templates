"""SqlContextReceiptStore: the same receipts in any SQLAlchemy database. SQLite and DuckDB run for real (no fake); Postgres, MySQL,
Snowflake and Databricks statements are only parsed with sqlglot (no such servers here), so passing proves syntax, not behaviour there."""
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
sys.path.insert(0, str(Path(__file__).parent))
from test_bigquery_backend import item, scenario, norm_usage  # noqa: E402


def _load():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_sql_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load()


@pytest.fixture(params=["sqlite", "duckdb", "dbapi-sqlite", "dbapi-duckdb"])
def sql(request, mod, tmp_path):
    if request.param.startswith("dbapi-"):      # the same suite on a plain PEP-249 connection (no SQLAlchemy)
        import sqlite3
        if request.param == "dbapi-duckdb":
            duckdb = pytest.importorskip("duckdb")
            store = mod.SqlContextReceiptStore(connection_factory=lambda: duckdb.connect(str(tmp_path / "r.duckdb")), dialect="duckdb", table_prefix="g_")
        else:
            store = mod.SqlContextReceiptStore(connection_factory=lambda: sqlite3.connect(str(tmp_path / "g.sqlite3")), dialect="sqlite", table_prefix="g_")
        yield store
        store.close()
        return
    if request.param == "duckdb":
        pytest.importorskip("duckdb_engine")
        engine = sqlalchemy.create_engine(f"duckdb:///{tmp_path / 'r.duckdb'}")
    else:
        engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'g.sqlite3'}")
    store = mod.SqlContextReceiptStore(engine, table_prefix="g_")
    yield store
    engine.dispose()


@pytest.fixture()
def lite(mod, tmp_path):
    return mod.ContextReceiptStore(str(tmp_path / "r.sqlite3"))


def test_parity_with_the_native_sqlite_store(sql, lite):
    scenario(sql)
    scenario(lite)
    since = "2000-01-01T00:00:00+00:00"
    for cv in (None, "cv1", "cv2"):
        got, want = sql.item_feedback("kb", since=since, code_version=cv), lite.item_feedback("kb", since=since, code_version=cv)
        for d in (got, want):
            for e in d.values():
                e.pop("last_retrieved_at")
        assert got == want, cv
    assert sql.outcomes_by_code_version("kb", since) == lite.outcomes_by_code_version("kb", since)
    assert norm_usage(sql.asset_usage_since("kb", since)) == norm_usage(lite.asset_usage_since("kb", since))
    assert sql.refresh_item_scores(3.0) == lite.refresh_item_scores(3.0)
    got, want = sql.get_item_scores(["a", "b", "c", "nope"]), lite.get_item_scores(["a", "b", "c", "nope"])
    assert set(got) == set(want) == {"a", "b", "c"}
    for k in want:
        for f in ("retrievals", "outcome_weight", "outcome_sum", "prior"):
            assert got[k][f] == pytest.approx(want[k][f]), (k, f)
    assert sql.find_by_item("a") == lite.find_by_item("a")
    assert sql.find_by_source_asset("kb") == lite.find_by_source_asset("kb")
    assert sql.find_by_assets({"kb": ["2026-01-01"]}) == lite.find_by_assets({"kb": ["2026-01-01"]})


def test_get_receipt_matches(sql, lite):
    scenario(sql)
    scenario(lite)
    g, w = sql.get_receipt("r1"), lite.get_receipt("r1")
    for r in (g, w):
        r.pop("created_at")
        for e in r["events"]:
            e.pop("event_id"), e.pop("occurred_at")
    assert g == w
    assert sql.get_receipt("nope") is None


def test_second_refresh_replaces_rows_not_duplicates(sql):
    scenario(sql)
    sql.refresh_item_scores(1.0)
    before = sql.get_item_scores(["a"])["a"]["prior"]
    sql.record_retrieval(context_name="kb", receipt_id="r9", items=[item("a")])
    sql.record_outcome("r9", signal="thumbs_down")
    assert sql.refresh_item_scores(1.0) == 3
    assert sql.get_item_scores(["a"])["a"]["prior"] < before
    assert sql._query(f"SELECT COUNT(1) AS n FROM {sql._t('context_item_scores')} WHERE item_id = 'a'")[0]["n"] == 1


def test_duplicate_unknown_and_validation(sql, mod):
    sql.record_retrieval(context_name="kb", receipt_id="dup", items=[item("a")])
    with pytest.raises(mod.DuplicateReceiptError):
        sql.record_retrieval(context_name="kb", receipt_id="dup", items=[item("a")])
    with pytest.raises(KeyError):
        sql.record_outcome("ghost", signal="thumbs_up")
    with pytest.raises(ValueError):
        sql.record_retrieval(context_name="kb", items=[item("a"), item("a")])


def test_failed_write_rolls_back_the_whole_receipt(sql):
    """A receipt is one transaction: if an insert fails midway nothing of it is left behind."""
    sql.ensure_tables()
    sql._query(f"DROP TABLE {sql._t('context_receipts')}")      # the last insert (the receipt row) will fail
    with pytest.raises(Exception):
        sql.record_retrieval(context_name="kb", receipt_id="tx", items=[item("a")])
    assert sql._query(f"SELECT COUNT(1) AS n FROM {sql._t('context_receipt_items')} WHERE receipt_id = 'tx'")[0]["n"] == 0


def test_read_since_cursors_resume_without_loss_or_repeat_and_carry_every_event_type(sql):
    for i in range(5):
        sql.record_retrieval(context_name="kb", receipt_id=f"p{i}", items=[item("a", rank=i)])
    sql.record_outcome("p0", signal="thumbs_up")
    sql.add_evidence("p1", level="cited", details={})
    sql.attach_output("p2", output_id="o", model="m", output_hash="h")
    seen, rc, ec, kinds = [], 0, 0, []
    for _ in range(12):
        page = sql.read_since(rc, ec, limit=2)
        seen += [r["receipt_id"] for r in page["receipts"]]
        kinds += [e["event_type"] for e in page["events"]]
        if not page["receipts"] and not page["events"]:
            break
        rc, ec = page["receipt_cursor"], page["event_cursor"]
    assert sorted(seen) == [f"p{i}" for i in range(5)]
    assert sorted(kinds) == ["evidence", "outcome", "output_attached"]
    sql.record_outcome("p3", signal="thumbs_down")
    page = sql.read_since(rc, ec)
    assert [o["receipt_id"] for o in page["outcomes"]] == ["p3"] and page["receipts"] == []
    assert page["outcomes"][0]["sources"] == [("kb", None, "cv1")]
    assert set(page["events"][0]) == {"event_id", "receipt_id", "event_type", "payload_json", "occurred_at"}


def test_enforce_retention_and_implicit_outcomes_match_sqlite(sql, lite):
    scenario(sql)
    scenario(lite)
    future = datetime.now(timezone.utc) + timedelta(days=40)
    assert sql.enforce_retention({"kb": 30}, now=future) == lite.enforce_retention({"kb": 30}, now=future) == {"receipts_deleted": 4}
    assert sql.find_by_source_asset("kb") == []
    for s in (sql, lite):
        s.record_retrieval(context_name="kb", receipt_id="i1", items=[item("a")], mcp_session_id="s", caller_id="u", query_hash="h1")
        s.record_retrieval(context_name="kb", receipt_id="i2", items=[item("a")], mcp_session_id="s", caller_id="u", query_hash="h2")
    assert sql.derive_implicit_outcomes() == lite.derive_implicit_outcomes() == 1
    assert sql.derive_implicit_outcomes() == 0
    assert sql.outcomes_by_code_version("kb") == lite.outcomes_by_code_version("kb")


def test_ingest_and_export_roundtrip(sql):
    sql.ingest({"spec_version": "1", "type": "retrieval", "receipt_id": "sp1", "context_name": "kb", "items": [item("a")]})
    sql.ingest({"spec_version": "1", "type": "outcome", "receipt_id": "sp1", "signal": "thumbs_up"})
    assert sql.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    assert [e["type"] for e in sql.export_events("sp1")] == ["retrieval", "outcome"]


def test_unsupported_dialect_is_a_clear_error(mod):
    class E:
        class dialect:  # noqa: N801
            name = "oracle"
    with pytest.raises(ValueError, match="unsupported SQL dialect"):
        mod.SqlContextReceiptStore(E())


def test_engine_factory_is_lazy_and_component_wires_backend_sql(mod, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setenv("RECEIPTS_URL", f"sqlite:///{tmp_path / 'c.sqlite3'}")
    comp = mod.ContextReceiptStoreComponent(backend="sql", sql_url_env_var="RECEIPTS_URL", async_writes=False, sql_table_prefix="c_")
    store = comp.build_defs(None).resources["context_receipts"].resource_fn(None)
    assert isinstance(store, mod.SqlContextReceiptStore) and store._engine_obj is None    # nothing connected at load time
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.record_outcome(rid, signal="thumbs_up")
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    with pytest.raises(ValueError, match="sql_url_env_var"):
        mod.ContextReceiptStoreComponent(backend="sql").build_defs(None)


def test_async_sql_store(mod, tmp_path):
    store = mod.AsyncSqlContextReceiptStore(f"sqlite:///{tmp_path / 'a.sqlite3'}")
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.record_outcome(rid, signal="thumbs_down")
    store.flush()
    assert store.dead_letters == [] and store.item_feedback("kb")["a"]["mean_outcome"] == -1.0


@pytest.mark.parametrize("dialect,read", [("postgres", "postgres"), ("mysql", "mysql"), ("snowflake", "snowflake"),
                                          ("databricks", "databricks"), ("duckdb", "duckdb"), ("sqlite", "sqlite")])
def test_every_statement_parses_in_each_dialect(mod, dialect, read):
    """No such servers here: record every statement the store would send and parse it as that dialect."""
    import sqlglot

    class Recorder(mod.SqlContextReceiptStore):
        def __init__(self):
            super().__init__(lambda: None, schema="db.sch" if dialect in ("snowflake", "databricks") else "sch", table_prefix="p_", dialect=dialect)
            self.seen = []

        def _query(self, sql, **params):
            self.seen.append(sql)
            return []

        def ensure_tables(self):
            self.seen.extend(mod.receipt_table_ddl(self.schema, self.table_prefix, self.dialect))

    r = Recorder()
    r.ensure_tables()
    r.get_receipt("x")
    r.item_feedback("kb", since="2026-01-01", code_version="cv")
    r.asset_usage_since("kb", "2026-01-01")
    r.outcomes_by_code_version("kb")
    r.find_by_assets({"kb": None})
    r.find_by_item("a")
    r.find_by_source_asset("kb")
    r.derive_implicit_outcomes()
    assert len(r.seen) > 12
    for q in r.seen:
        # SQLAlchemy `:name` and `IN :ids` binds are not SQL; swap them for literals before parsing
        import re
        q2 = re.sub(r"IN :\w+", "IN ('x')", q)
        q2 = re.sub(r":\w+", "'x'", q2)
        sqlglot.parse(q2, read=read)


def test_mysql_ddl_quotes_the_reserved_rank_column(mod):
    ddl = "\n".join(mod.receipt_table_ddl("db", "p_", "mysql"))
    assert "`rank` BIGINT" in ddl and "{K}" not in ddl
    assert "\n".join(mod.receipt_table_ddl("db", "p_", "postgres")).count(" rank BIGINT") == 1


def test_missing_sql_driver_names_exactly_what_to_install(mod, monkeypatch):
    import sqlalchemy
    from sqlalchemy.exc import NoSuchModuleError

    def boom(url, **kw):
        raise NoSuchModuleError("Can't load plugin: sqlalchemy.dialects:snowflake")
    monkeypatch.setattr(sqlalchemy, "create_engine", boom)
    with pytest.raises(ImportError, match="pip install sqlalchemy snowflake-sqlalchemy"):
        mod._create_sql_engine("snowflake://u:p@acct/db")
    with pytest.raises(ImportError, match="psycopg2-binary"):
        mod._create_sql_engine("postgresql+psycopg2://u@h/db")
