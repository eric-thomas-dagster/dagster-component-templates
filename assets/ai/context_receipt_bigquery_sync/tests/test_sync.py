"""ContextReceiptBigQuerySyncComponent against the REAL ContextReceiptStore and a DuckDB-backed fake BigQuery that executes the real SQL
(parsed as BigQuery by sqlglot, so invalid BigQuery SQL fails here). Only BigQuery itself is faked."""
import sqlite3
from datetime import datetime, timezone

import dagster as dg
import pytest
from bq_fake import FakeBigQueryResource

DS = "proj.ctx"


def _utc(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(v)


def component(comp, **kw):
    kw.setdefault("store_resource_key", "receipts")
    kw.setdefault("bigquery_resource_key", "bigquery")
    kw.setdefault("dataset", DS)
    return comp.ContextReceiptBigQuerySyncComponent(**kw)


def run_asset(comp, store, client, **kw):
    """Real Dagster materialize with the store and a dagster-gcp-shaped BigQuery resource."""
    defs = component(comp, **kw).build_defs(None)
    r = dg.materialize(list(defs.assets), resources={"receipts": store, "bigquery": FakeBigQueryResource(client)}, raise_on_error=False)
    assert r.success, [e.event_specific_data for e in r.all_events if e.is_failure]
    return r.asset_materializations_for_node("context_receipt_bigquery_sync")[0].metadata


def seed(store, n=3, prefix="r"):
    ids = []
    for i in range(n):
        ids.append(store.record_retrieval(
            context_name="kb", items=[{"item_id": f"{prefix}{i}-a", "source_asset_key": "docs", "source_version": "v1", "rank": 0, "score": 0.9},
                                      {"item_id": f"{prefix}{i}-b", "source_asset_key": "docs", "source_partition": "p1", "rank": 1, "score": 0.5}],
            excluded=[{"item_id": f"{prefix}{i}-x", "reason": "pii", "source_asset_key": "docs"}],
            caller_id="u1", purpose="support", query_hash=f"h{i}", eligible_count=3, dagster_asset_key="docs", trace_id=f"t{i}"))
    return ids


def sqlite_rows(store, table, order):
    con = sqlite3.connect(store.database_path)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(f"SELECT * FROM {table} ORDER BY {order}")]
    finally:
        con.close()


def bq_rows(client, table, order):
    cur = client.con.execute(f"SELECT * FROM ctx.{table} ORDER BY {order}")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def normalize(rows):
    out = []
    for r in rows:
        r = dict(r)
        for k in ("created_at", "occurred_at"):
            if k in r:
                r[k] = _utc(r[k])
        out.append(r)
    return out


def assert_mirrors(store, client):
    spec = {"context_receipts": "receipt_id", "context_receipt_items": "receipt_id, item_id, rank",
            "context_receipt_excluded": "receipt_id, item_id", "context_receipt_events": "event_id"}
    for table, order in spec.items():
        mine = normalize(sqlite_rows(store, table, order))
        theirs = normalize(bq_rows(client, table, order))
        assert theirs == mine, table


def count(client, table):
    return client.con.execute(f"SELECT COUNT(*) FROM ctx.{table}").fetchone()[0]


def state(client):
    return client.con.execute("SELECT receipt_cursor, event_cursor FROM ctx.context_sync_state ORDER BY updated_at DESC").fetchall()


def test_full_sync_equals_store_including_non_outcome_events(comp, store, bq):
    client, _ = bq
    ids = seed(store)
    store.attach_output(ids[0], output_id="o1", model="m", output_hash="hh")
    store.add_evidence(ids[0], level="cited", details={"item_ids": [f"r0-a"]})
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u1")
    md = run_asset(comp, store, client)
    assert_mirrors(store, client)
    types = {r[0] for r in client.con.execute("SELECT event_type FROM ctx.context_receipt_events").fetchall()}
    assert types == {"output_attached", "evidence", "outcome"}
    assert md["rows_context_receipts"].value == 3 and md["rows_context_receipt_items"].value == 6
    assert md["rows_context_receipt_excluded"].value == 3 and md["rows_context_receipt_events"].value == 3
    assert md["receipt_cursor_before"].value == 0 and md["receipt_cursor_after"].value == 3 and md["event_cursor_after"].value == 3  # rowid over ALL event types
    assert md["drained"].value is True and md["lag_seconds"].value >= 0
    assert len(state(client)) == 1 and state(client)[0] == (3, 3)


def test_payload_json_is_byte_identical_and_timestamps_are_timestamps(comp, store, bq):
    client, _ = bq
    ids = seed(store, 1)
    store.record_outcome(ids[0], signal="thumbs_down", outcome_ref="u1", source="ui")
    run_asset(comp, store, client)
    want = sqlite_rows(store, "context_receipt_events", "event_id")[0]["payload_json"]
    assert client.con.execute("SELECT payload_json FROM ctx.context_receipt_events").fetchone()[0] == want
    assert "TIMESTAMP" in client.con.execute("SELECT typeof(created_at) FROM ctx.context_receipts").fetchone()[0].upper()


def test_incremental_second_run_ships_only_new(comp, store, bq):
    client, _ = bq
    seed(store, 2)
    run_asset(comp, store, client)
    md = run_asset(comp, store, client)
    assert md["rows_context_receipts"].value == 0 and md["rows_context_receipt_events"].value == 0
    assert md["receipt_cursor_before"].value == 2 and md["receipt_cursor_after"].value == 2
    seed(store, 2, prefix="n")
    md = run_asset(comp, store, client)
    assert md["rows_context_receipts"].value == 2 and md["rows_context_receipt_items"].value == 4
    assert md["receipt_cursor_before"].value == 2 and md["receipt_cursor_after"].value == 4
    assert_mirrors(store, client)


def test_late_outcome_for_old_receipt_is_shipped(comp, store, bq):
    client, _ = bq
    ids = seed(store, 2)
    run_asset(comp, store, client)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="late")
    md = run_asset(comp, store, client)
    assert md["rows_context_receipts"].value == 0 and md["rows_context_receipt_events"].value == 1
    assert_mirrors(store, client)


def test_late_evidence_and_output_without_outcome_is_shipped_with_no_reconcile_window(comp, store, bq):
    client, _ = bq
    ids = seed(store, 2)
    run_asset(comp, store, client)
    store.add_evidence(ids[1], level="verified", details={})  # no new receipt, no outcome
    md = run_asset(comp, store, client)
    assert md["rows_context_receipt_events"].value == 1 and md["rows_context_receipts"].value == 0
    store.attach_output(ids[0], output_id="o9", model="m", output_hash="zz")
    md = run_asset(comp, store, client)
    assert md["rows_context_receipt_events"].value == 1
    assert_mirrors(store, client)


def test_events_come_from_read_since_without_get_receipt(comp, store, bq):
    client, mod = bq
    ids = seed(store, 2)
    store.add_evidence(ids[0], level="cited", details={})
    class NoGetReceipt:
        read_since = staticmethod(store.read_since)
        def get_receipt(self, *a, **k):
            raise AssertionError("sync must not re-fetch receipts for events")
    comp.sync_receipts(NoGetReceipt(), client, mod, dataset=DS)
    assert_mirrors(store, client)


def test_deprecated_reconcile_field_is_accepted_and_ignored(comp, store, bq):
    client, _ = bq
    seed(store, 1)
    with pytest.warns(DeprecationWarning):
        md = run_asset(comp, store, client, reconcile_recent_receipts=200)
    assert md["rows_context_receipts"].value == 1
    assert component(comp).reconcile_recent_receipts == 0


def test_events_paging_over_non_outcome_events_drains(comp, store, bq):
    client, _ = bq
    ids = seed(store, 1)
    for i in range(5):
        store.add_evidence(ids[0], level="cited", details={"n": i})
    md = run_asset(comp, store, client, batch_size=2, max_batches=50)
    assert md["drained"].value is True and md["batches"].value == 3
    assert_mirrors(store, client)


def test_empty_store(comp, store, bq):
    client, _ = bq
    md = run_asset(comp, store, client)
    assert md["rows_context_receipts"].value == 0 and md["batches"].value == 0 and md["drained"].value is True
    assert "lag_seconds" not in md
    assert count(client, "context_receipts") == 0 and state(client) == []


def test_batch_limit_and_max_batches_resume(comp, store, bq):
    client, _ = bq
    seed(store, 5)
    md = run_asset(comp, store, client, batch_size=2, max_batches=2)
    assert md["batches"].value == 2 and md["drained"].value is False
    assert count(client, "context_receipts") == 4 and md["receipt_cursor_after"].value == 4
    md = run_asset(comp, store, client, batch_size=2, max_batches=2)
    assert md["rows_context_receipts"].value == 1 and md["drained"].value is True
    assert_mirrors(store, client)


def test_many_batches_drain_with_small_batch_size(comp, store, bq):
    client, _ = bq
    ids = seed(store, 5)
    for i in ids:
        store.record_outcome(i, signal="thumbs_up", outcome_ref="u")
    md = run_asset(comp, store, client, batch_size=2, max_batches=50)
    assert md["drained"].value is True and md["batches"].value == 3
    assert_mirrors(store, client)


class Boom(Exception):
    pass


def test_crash_between_load_and_cursor_advance_replays_without_duplicates(comp, store, bq, monkeypatch):
    client, mod = bq
    ids = seed(store, 3)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u")
    store.add_evidence(ids[0], level="cited", details={})
    real = comp._write_state
    def crash(*a, **k):
        raise Boom()
    monkeypatch.setattr(comp, "_write_state", crash)
    store_proxy = store
    with pytest.raises(Boom):
        comp.sync_receipts(store_proxy, client, mod, dataset=DS)
    assert count(client, "context_receipts") == 3 and state(client) == []  # data loaded, cursor not advanced
    monkeypatch.setattr(comp, "_write_state", real)
    comp.sync_receipts(store_proxy, client, mod, dataset=DS)  # replay
    comp.sync_receipts(store_proxy, client, mod, dataset=DS)  # and again
    assert (count(client, "context_receipts"), count(client, "context_receipt_items"), count(client, "context_receipt_excluded"),
            count(client, "context_receipt_events")) == (3, 6, 3, 2)
    assert_mirrors(store, client)
    assert len(state(client)) == 1


def test_crash_midway_between_table_loads_replays_cleanly(comp, store, bq, monkeypatch):
    """Receipts loaded but the process died before items/excluded/events: replay must end exactly equal to the store."""
    client, mod = bq
    seed(store, 3)
    real = client.load_table_from_json
    def flaky(rows, destination, **kw):
        if "context_receipt_items" in str(destination):
            raise Boom()
        return real(rows, destination, **kw)
    monkeypatch.setattr(client, "load_table_from_json", flaky)
    with pytest.raises(Boom):
        comp.sync_receipts(store, client, mod, dataset=DS)
    monkeypatch.setattr(client, "load_table_from_json", real)
    comp.sync_receipts(store, client, mod, dataset=DS)
    assert_mirrors(store, client)


@pytest.mark.parametrize("mutation,table", [("delete", "context_receipts"), ("delete", "context_receipt_items"), ("event_diff", "context_receipt_events")])
def test_idempotency_mutation_check(comp, store, bq, monkeypatch, mutation, table):
    """The replay tests must be able to fail: dropping the DELETE-per-receipt or the event_id diff makes a replay duplicate rows."""
    client, mod = bq
    ids = seed(store, 2)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u")
    real_query, real_state = client.query, comp._write_state
    def mutated(sql, *a, **k):
        if mutation == "delete" and sql.lstrip().startswith("DELETE FROM `proj.ctx.context_receipt"):
            return real_query("SELECT 1")
        if mutation == "event_diff" and "SELECT event_id FROM" in sql:
            return real_query("SELECT 'x' AS event_id WHERE 1=0")
        return real_query(sql, *a, **k)
    monkeypatch.setattr(comp, "_write_state", lambda *a, **k: (_ for _ in ()).throw(Boom()))
    with pytest.raises(Boom):
        comp.sync_receipts(store, client, mod, dataset=DS)
    monkeypatch.setattr(comp, "_write_state", real_state)
    monkeypatch.setattr(client, "query", mutated)
    comp.sync_receipts(store, client, mod, dataset=DS)
    assert count(client, table) > len(sqlite_rows(store, table, "1"))  # mutated code duplicates

def test_all_sql_is_valid_bigquery_and_uses_load_jobs(comp, store, bq):
    import sqlglot
    client, _ = bq
    ids = seed(store, 2)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u")
    run_asset(comp, store, client)
    assert client.queries
    for sql in client.queries:
        sqlglot.parse(sql, read="bigquery", error_level=sqlglot.ErrorLevel.RAISE)
    assert any("UNNEST(@ids)" in q for q in client.queries)


def test_table_prefix_and_dataset_without_project(comp, store, bq):
    client, _ = bq
    seed(store, 2)
    run_asset(comp, store, client, dataset="ctx", table_prefix="p_")
    assert client.con.execute("SELECT COUNT(*) FROM ctx.p_context_receipts").fetchone()[0] == 2
    assert client.con.execute("SELECT COUNT(*) FROM ctx.p_context_sync_state").fetchone()[0] == 1


def test_two_syncs_keep_independent_cursors(comp, store, bq):
    client, mod = bq
    seed(store, 2)
    comp.sync_receipts(store, client, mod, dataset=DS, component_name="a")
    seed(store, 1, prefix="z")
    comp.sync_receipts(store, client, mod, dataset=DS, component_name="a")
    rows = client.con.execute("SELECT component_name, receipt_cursor FROM ctx.context_sync_state").fetchall()
    assert rows == [("a", 3)]


def test_config_options_apply(comp):
    c = component(comp, retry_policy_max_retries=3, retry_policy_backoff="linear", freshness_max_lag_minutes=30, owners="team:a, b@x.com",
                  kinds="bigquery", deps=["receipts_store"], group_name="g", cron_schedule="*/10 * * * *")
    assert c.owners == ["team:a", "b@x.com"]
    defs = c.build_defs(None)
    spec = list(defs.assets)[0]
    assert spec.op.retry_policy.max_retries == 3
    assert defs.schedules and list(defs.assets)[0].group_names_by_key
    with pytest.raises(ValueError):
        component(comp, retry_policy_backoff="fixed").build_defs(None)
    with pytest.raises(ValueError):
        component(comp, batch_size=0).build_defs(None)


def test_ddl_block_is_verbatim_the_store_components_block(comp):
    import pathlib, re
    root = pathlib.Path(__file__).resolve().parents[4]
    pat = r"# BEGIN receipt-warehouse-ddl.*?# END receipt-warehouse-ddl"
    mine = re.search(pat, (pathlib.Path(__file__).resolve().parent.parent / "component.py").read_text(), re.S)
    ref = re.search(pat, (root / "resources/context_receipt_store/component.py").read_text(), re.S)
    assert mine and ref and mine.group(0) == ref.group(0)
