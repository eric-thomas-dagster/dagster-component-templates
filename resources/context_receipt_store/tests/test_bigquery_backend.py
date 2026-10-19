"""BigQueryContextReceiptStore: the same receipts in BigQuery, held to the SQLite store's results by a parity run.

There is no BigQuery here. Every statement the store sends is parsed as BigQuery by sqlglot and run in DuckDB (bq_fake.py), so
the SQL must be valid BigQuery and the logic is really executed -- but dialect corners can still differ on real BigQuery.
"""
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .bq_fake import FakeBigQueryClient, install_fake_bigquery  # noqa: E402


def _load():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_bq_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load()


@pytest.fixture()
def client(monkeypatch):
    c = FakeBigQueryClient("proj")
    c.create_dataset("proj.ds")
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", install_fake_bigquery(c))
    return c


@pytest.fixture()
def bq(mod, client):
    return mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="t_")


@pytest.fixture()
def lite(mod, tmp_path):
    return mod.ContextReceiptStore(str(tmp_path / "r.sqlite3"))


def item(chunk, cv="cv1", asset="kb", rank=0, partition=None):
    return {"item_id": chunk, "source_asset_key": asset, "source_version": "v1", "source_code_version": cv, "rank": rank,
            "score": 0.5, "source_partition": partition}


def scenario(store):
    """Identical operations against any store; returns the receipt ids in order."""
    ids = []
    ids.append(store.record_retrieval(context_name="kb", receipt_id="r1", items=[item("a", rank=0), item("b", rank=1)],
                                      excluded=[{"item_id": "z", "reason": "pii", "source_asset_key": "kb", "source_code_version": "cv1"}],
                                      caller_id="u1", dagster_asset_key="kb", mcp_session_id="s1", query_hash="q1"))
    ids.append(store.record_retrieval(context_name="kb", receipt_id="r2", items=[item("a")], caller_id="u2"))
    ids.append(store.record_retrieval(context_name="kb", receipt_id="r3", items=[item("a", cv="cv2"), item("c", cv="cv2", rank=1)]))
    ids.append(store.record_retrieval(context_name="kb", receipt_id="r4", items=[item("a", partition="2026-01-01")]))
    store.record_outcome("r1", signal="thumbs_down", outcome_ref="u1")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")      # supersedes the earlier outcome for the same ref
    store.record_outcome("r1", signal="edited", outcome_ref="u9")
    store.record_outcome("r2", signal="thumbs_up")
    store.record_outcome("r3", signal="thumbs_down")
    store.add_evidence("r3", level="cited", details={"item_ids": ["c"]})
    store.add_evidence("r1", level="verified", details={"claims_total": 4, "claims_ungrounded": 1})
    store.attach_output("r1", output_id="o1", model="m", output_hash="h")
    return ids


def norm_usage(u):
    return {**u, "items": sorted(u["items"], key=str), "outcome_means": sorted(round(x, 6) for x in u["outcome_means"])}


def test_parity_with_the_sqlite_store(bq, lite):
    scenario(bq)
    scenario(lite)
    since = "2000-01-01T00:00:00+00:00"
    for cv in (None, "cv1", "cv2"):
        got, want = bq.item_feedback("kb", since=since, code_version=cv), lite.item_feedback("kb", since=since, code_version=cv)
        for d in (got, want):
            for e in d.values():
                e.pop("last_retrieved_at")
        assert got == want, cv
    assert bq.outcomes_by_code_version("kb", since) == lite.outcomes_by_code_version("kb", since)
    assert norm_usage(bq.asset_usage_since("kb", since)) == norm_usage(lite.asset_usage_since("kb", since))
    assert bq.refresh_item_scores(3.0) == lite.refresh_item_scores(3.0)
    got, want = bq.get_item_scores(["a", "b", "c", "nope"]), lite.get_item_scores(["a", "b", "c", "nope"])
    assert set(got) == set(want) == {"a", "b", "c"}
    for k in want:
        for f in ("retrievals", "outcome_weight", "outcome_sum", "prior"):
            assert got[k][f] == pytest.approx(want[k][f]), (k, f)
    assert bq.find_by_item("a") == lite.find_by_item("a")
    assert bq.find_by_source_asset("kb") == lite.find_by_source_asset("kb")
    assert bq.find_by_assets({"kb": ["2026-01-01"]}) == lite.find_by_assets({"kb": ["2026-01-01"]})
    assert bq.find_by_assets({"kb": ["x"]}) == lite.find_by_assets({"kb": ["x"]})


def test_get_receipt_matches(bq, lite):
    scenario(bq)
    scenario(lite)
    g, w = bq.get_receipt("r1"), lite.get_receipt("r1")
    for r in (g, w):
        r.pop("created_at")
        for e in r["events"]:
            e.pop("event_id"), e.pop("occurred_at")
    assert g == w
    assert bq.get_receipt("nope") is None


def test_second_refresh_updates_scores_through_merge(bq):
    scenario(bq)
    bq.refresh_item_scores(1.0)
    before = bq.get_item_scores(["a"])["a"]["prior"]
    bq.record_retrieval(context_name="kb", receipt_id="r9", items=[item("a")])
    bq.record_outcome("r9", signal="thumbs_down")
    assert bq.refresh_item_scores(1.0) == 3
    assert bq.get_item_scores(["a"])["a"]["prior"] < before        # the new bad outcome moved it, no duplicate row
    n = bq._query(f"SELECT COUNT(1) AS n FROM {bq._t('context_item_scores')} WHERE item_id = 'a'")[0]["n"]
    assert n == 1


def test_duplicate_receipt_and_unknown_receipt(bq, mod):
    bq.record_retrieval(context_name="kb", receipt_id="dup", items=[item("a")])
    with pytest.raises(mod.DuplicateReceiptError):
        bq.record_retrieval(context_name="kb", receipt_id="dup", items=[item("a")])
    with pytest.raises(KeyError):
        bq.record_outcome("ghost", signal="thumbs_up")
    with pytest.raises(ValueError):
        bq.record_retrieval(context_name="kb", items=[item("a"), item("a")])      # shared validation


def test_retry_after_a_crash_between_items_and_receipt_leaves_no_duplicate_items(bq, mod):
    """Items load before the receipt row (the commit marker); a retry with the same id must clean up the orphans."""
    bq._load(bq._t("context_receipt_items"), [{"receipt_id": "crash", **item("a"), "selection": "ranked"}],
             [("receipt_id", "STRING"), ("item_id", "STRING"), ("source_asset_key", "STRING"), ("source_version", "STRING"),
              ("source_partition", "STRING"), ("source_code_version", "STRING"), ("rank", "INT64"), ("score", "FLOAT64"), ("selection", "STRING")])
    bq.record_retrieval(context_name="kb", receipt_id="crash", items=[item("a")])
    assert bq._query(f"SELECT COUNT(1) AS n FROM {bq._t('context_receipt_items')} WHERE receipt_id = 'crash'")[0]["n"] == 1


def test_read_since_cursors_resume_without_loss_or_repeat(bq):
    for i in range(5):
        bq.record_retrieval(context_name="kb", receipt_id=f"p{i}", items=[item("a", rank=i)])
    bq.record_outcome("p0", signal="thumbs_up")
    seen, rc, ec, outcomes = [], 0, 0, 0
    for _ in range(10):
        page = bq.read_since(rc, ec, limit=2)
        seen += [r["receipt_id"] for r in page["receipts"]]
        outcomes += len(page["outcomes"])
        if not page["receipts"] and not page["outcomes"]:
            break
        rc, ec = page["receipt_cursor"], page["event_cursor"]
    assert sorted(seen) == [f"p{i}" for i in range(5)] and outcomes == 1
    bq.record_outcome("p3", signal="thumbs_down")                    # a late outcome for an old receipt arrives via the event cursor
    page = bq.read_since(rc, ec)
    assert [o["receipt_id"] for o in page["outcomes"]] == ["p3"] and page["receipts"] == []
    assert page["outcomes"][0]["sources"] == [("kb", None, "cv1")]


def test_enforce_retention_matches_sqlite(bq, lite):
    scenario(bq)
    scenario(lite)
    future = datetime.now(timezone.utc) + timedelta(days=40)
    assert bq.enforce_retention({"kb": 30}, now=future) == lite.enforce_retention({"kb": 30}, now=future) == {"receipts_deleted": 4}
    assert bq.find_by_source_asset("kb") == []
    assert bq.enforce_retention({}, now=future) == {"receipts_deleted": 0}


def test_implicit_reformulation_matches_sqlite(bq, lite):
    for s in (bq, lite):
        s.record_retrieval(context_name="kb", receipt_id="i1", items=[item("a")], mcp_session_id="s", caller_id="u", query_hash="h1")
        s.record_retrieval(context_name="kb", receipt_id="i2", items=[item("a")], mcp_session_id="s", caller_id="u", query_hash="h2")
    assert bq.derive_implicit_outcomes() == lite.derive_implicit_outcomes() == 1
    assert bq.derive_implicit_outcomes() == 0                         # idempotent
    assert bq.outcomes_by_code_version("kb") == lite.outcomes_by_code_version("kb")


def test_ingest_spec_events_through_the_bigquery_store(bq):
    ev = {"spec_version": "1", "type": "retrieval", "receipt_id": "sp1", "context_name": "kb", "items": [item("a")]}
    assert bq.ingest(ev) == "sp1"
    bq.ingest({"spec_version": "1", "type": "outcome", "receipt_id": "sp1", "signal": "thumbs_up"})
    assert bq.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    kinds = [e["type"] for e in bq.export_events("sp1")]
    assert kinds == ["retrieval", "outcome"]


def test_sql_is_valid_bigquery_for_every_statement(bq, client):
    scenario(bq)
    bq.refresh_item_scores()
    bq.item_feedback("kb")
    bq.enforce_retention({"kb": 1}, now=datetime.now(timezone.utc) + timedelta(days=3))
    import sqlglot
    assert len(client.queries) > 25
    for q in client.queries:
        sqlglot.parse(q, read="bigquery")                              # raises on anything that is not valid BigQuery


def test_missing_bigquery_library_is_a_clear_error(mod, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def no_google(name, *args, **kwargs):
        if name.startswith("google.cloud"):
            raise ImportError("No module named 'google.cloud.bigquery'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_google)
    store = mod.BigQueryContextReceiptStore("proj.ds", client_factory=lambda: None)
    with pytest.raises(ImportError):
        store.find_by_item("a")


def test_dataset_must_be_project_dot_dataset(mod):
    with pytest.raises(ValueError):
        mod.BigQueryContextReceiptStore("justdataset", client_factory=lambda: None)


def test_async_store_returns_immediately_and_flush_gives_read_your_writes(mod, client):
    store = mod.AsyncBigQueryContextReceiptStore("proj.ds", client_factory=lambda: client, table_prefix="a_")
    rid = store.record_retrieval(context_name="kb", items=[item("a")])
    store.record_outcome(rid, signal="thumbs_up")
    store.flush()
    assert store.dead_letters == []
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    with pytest.raises(ValueError):                                   # argument errors still surface at the call site
        store.record_retrieval(context_name="kb", items=[item("a"), item("a")])


def test_async_store_dead_letters_unknown_receipt(mod, client):
    store = mod.AsyncBigQueryContextReceiptStore("proj.ds", client_factory=lambda: client)
    store.record_outcome("ghost", signal="thumbs_up")
    store.flush()
    assert len(store.dead_letters) == 1


def test_component_validates_backend(mod):
    import dagster as dg
    C = mod.ContextReceiptStoreComponent
    with pytest.raises(ValueError, match="bigquery_dataset"):
        C(backend="bigquery").build_defs(None)
    with pytest.raises(ValueError, match="backend must be"):
        C(backend="redis").build_defs(None)
    defs = C(backend="bigquery", bigquery_dataset="proj.ds", bigquery_table_prefix="p_").build_defs(None)
    store = defs.resources["context_receipts"].resource_fn(None)
    assert isinstance(store, mod.AsyncBigQueryContextReceiptStore) and store.table_prefix == "p_"
