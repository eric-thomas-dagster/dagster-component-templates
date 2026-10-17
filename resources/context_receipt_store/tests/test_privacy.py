"""Privacy: keyed pseudonyms for caller/session ids at write time (every backend, sync and async) and default retention."""
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import KINDS, all_cells, load_module, make_store  # noqa: E402
from test_bigquery_backend import item  # noqa: E402

RAW_CALLER, RAW_SESSION = "alice@example.com", "sess-RAW-0001"


@pytest.fixture()
def mod():
    return load_module()


@pytest.fixture(params=KINDS)
def store(request, mod, tmp_path, monkeypatch):
    return make_store(request.param, mod, tmp_path, monkeypatch)


def test_raw_ids_never_reach_any_table_and_sessions_still_group(store):
    store.enable_pseudonymization("pepper")
    store.record_retrieval(context_name="kb", receipt_id="p1", items=[item("a")], caller_id=RAW_CALLER, mcp_session_id=RAW_SESSION, query_hash="q1")
    store.record_retrieval(context_name="kb", receipt_id="p2", items=[item("a")], caller_id=RAW_CALLER, mcp_session_id=RAW_SESSION, query_hash="q2")
    store.record_retrieval(context_name="kb", receipt_id="p3", items=[item("a")], caller_id="bob", mcp_session_id="other-session", query_hash="q3")
    store.record_outcome("p1", signal="thumbs_up")
    store.add_evidence("p1", level="cited", details={})
    store.refresh_item_scores()
    cells = " | ".join(all_cells(store))
    for raw in (RAW_CALLER, RAW_SESSION, "bob", "other-session"):
        assert raw not in cells, raw
    r1, r2, r3 = (store.get_receipt(r) for r in ("p1", "p2", "p3"))
    assert r1["caller_id"].startswith("h:") and len(r1["caller_id"]) == 2 + 32
    assert r1["mcp_session_id"].startswith("h:") and r1["mcp_session_id"] != r1["caller_id"]
    assert r1["caller_id"] == r2["caller_id"] != r3["caller_id"]            # deterministic, distinct people stay distinct
    assert r1["mcp_session_id"] == r2["mcp_session_id"] != r3["mcp_session_id"]
    # grouping by session+caller still works: p1 -> p2 within the window with a different query is a reformulation
    assert store.derive_implicit_outcomes(window_seconds=60) == 1
    assert [e["event_type"] for e in store.get_receipt("p1")["events"]].count("outcome") == 2
    assert store.derive_implicit_outcomes(window_seconds=60) == 0           # idempotent


def test_hash_is_keyed_hmac_sha256_prefix(mod, tmp_path):
    import hashlib
    import hmac
    a = mod.ContextReceiptStore(str(tmp_path / "a.db"))
    b = mod.ContextReceiptStore(str(tmp_path / "b.db"))
    a.enable_pseudonymization("salt-1")
    b.enable_pseudonymization("salt-2")
    assert a._pseudonym("x") == "h:" + hmac.new(b"salt-1", b"x", hashlib.sha256).hexdigest()[:32]
    assert a._pseudonym("x") != b._pseudonym("x")
    assert a._pseudonym(None) is None
    plain = mod.ContextReceiptStore(str(tmp_path / "c.db"))
    assert plain._pseudonym("x") == "x"                                      # off by default
    with pytest.raises(ValueError):
        a.enable_pseudonymization("")


def test_default_off_stores_raw_ids(store):
    store.record_retrieval(context_name="kb", receipt_id="p1", items=[item("a")], caller_id=RAW_CALLER)
    assert store.get_receipt("p1")["caller_id"] == RAW_CALLER


def test_unset_salt_refuses_to_write_a_raw_id(store):
    store.enable_pseudonymization(lambda: None)
    with pytest.raises(ValueError, match="salt"):
        store.record_retrieval(context_name="kb", receipt_id="p1", items=[item("a")], caller_id=RAW_CALLER)
    assert store.get_receipt("p1") is None


def test_ingest_path_is_pseudonymized_too(store):
    store.enable_pseudonymization("pepper")
    store.ingest({"spec_version": "1", "type": "retrieval", "receipt_id": "i1", "context_name": "kb", "items": [item("a")],
                  "caller_id": RAW_CALLER, "mcp_session_id": RAW_SESSION})
    assert RAW_CALLER not in " | ".join(all_cells(store))
    assert store.get_receipt("i1")["caller_id"].startswith("h:")


@pytest.mark.parametrize("kind", ["sql-sqlite", "dbapi-sqlite", "dbapi-duckdb", "bigquery-fake"])
def test_async_wrapper_pseudonymizes_before_queueing(kind, mod, tmp_path, monkeypatch):
    store = make_store(kind, mod, tmp_path, monkeypatch, async_writes=True)
    store.enable_pseudonymization("pepper")
    store.record_retrieval(context_name="kb", receipt_id="a1", items=[item("a")], caller_id=RAW_CALLER, mcp_session_id=RAW_SESSION)
    store.ingest({"spec_version": "1", "type": "retrieval", "receipt_id": "a2", "context_name": "kb", "items": [item("a")],
                  "caller_id": RAW_CALLER, "mcp_session_id": RAW_SESSION})
    for _op, _args, kwargs, *_ in list(store._writer._queue.queue):          # nothing raw is waiting in the queue (best effort: may be drained)
        assert RAW_CALLER not in repr(kwargs)
    store.flush()
    assert not store.dead_letters
    assert RAW_CALLER not in " | ".join(all_cells(store)) and RAW_SESSION not in " | ".join(all_cells(store))
    one, two = store.get_receipt("a1"), store.get_receipt("a2")
    assert one["caller_id"] == two["caller_id"] and one["caller_id"].startswith("h:")


# ── default retention ────────────────────────────────────────────────────────────────────────────────────────

def _retention_fixture(store):
    store.record_retrieval(context_name="kb", receipt_id="pol", items=[item("a", asset="has_policy")])
    store.record_retrieval(context_name="kb", receipt_id="own90", items=[item("a", asset="long_policy")])
    store.record_retrieval(context_name="kb", receipt_id="nopol", items=[item("a", asset="no_policy")])
    store.record_retrieval(context_name="kb", receipt_id="declared_none", items=[item("a", asset="none_declared")])
    store.record_retrieval(context_name="kb", receipt_id="noasset", items=[{"item_id": "x"}])
    store.record_outcome("nopol", signal="thumbs_up")


LIMITS = {"has_policy": 10, "long_policy": 90, "none_declared": None}
LATER = lambda: datetime.now(timezone.utc) + timedelta(days=40)   # noqa: E731


def _alive(store):
    return sorted(r for r in ("pol", "own90", "nopol", "declared_none", "noasset") if store.get_receipt(r) is not None)


def test_nothing_is_deleted_for_assets_without_retention_by_default(store):
    _retention_fixture(store)
    assert store.enforce_retention(LIMITS, now=LATER()) == {"receipts_deleted": 1}
    assert _alive(store) == ["declared_none", "noasset", "nopol", "own90"]


def test_default_days_covers_assets_with_no_declared_retention_but_the_assets_own_policy_wins(store):
    _retention_fixture(store)
    assert store.enforce_retention(LIMITS, now=LATER(), default_days=30) == {"receipts_deleted": 4}
    assert _alive(store) == ["own90"]                                       # 90-day policy beats the 30-day default
    assert store.get_receipt("nopol") is None
    # events/items of deleted receipts are gone with them
    assert not [c for c in all_cells(store) if c in ("nopol", "pol")]


def test_default_days_works_with_no_policies_at_all_and_rejects_nonsense(store):
    _retention_fixture(store)
    assert store.enforce_retention({}, now=LATER(), default_days=30) == {"receipts_deleted": 5}
    with pytest.raises(ValueError):
        store.enforce_retention({}, default_days=-1)


def test_sensor_applies_default_retention(mod, tmp_path):
    @mod.context_asset(retention_days=100000)
    @dg.asset(name="governed")
    def governed():
        ...

    @dg.asset(name="plain")
    def plain():
        ...

    defs = dg.Definitions(assets=[governed, plain])
    store = mod.ContextReceiptStore(str(tmp_path / "s.db"))
    store.record_retrieval(context_name="kb", receipt_id="g", items=[item("a", asset="governed")])
    store.record_retrieval(context_name="kb", receipt_id="p", items=[item("a", asset="plain")])
    ctx = lambda: dg.build_sensor_context(definitions=defs, instance=dg.DagsterInstance.ephemeral())   # noqa: E731
    off = mod.build_usage_sensor(store, name="s", interval_seconds=60, rebuild_asset_keys=[], stale_reads_threshold=99,
                                 prior_strength=1.0, enforce_retention=True)
    off(ctx())
    assert store.get_receipt("p") is not None                               # no default: kept forever
    on = mod.build_usage_sensor(store, name="s", interval_seconds=60, rebuild_asset_keys=[], stale_reads_threshold=99,
                                prior_strength=1.0, enforce_retention=True, default_retention_days=1e-9)
    on(ctx())
    assert store.get_receipt("p") is None and store.get_receipt("g") is not None


# ── component wiring and the load-time warning ───────────────────────────────────────────────────────────────

def test_component_wires_pseudonymization_and_requires_a_salt_env_var(mod, tmp_path, monkeypatch):
    C = mod.ContextReceiptStoreComponent
    with pytest.raises(ValueError, match="pseudonym_salt_env_var"):
        C(database_path=str(tmp_path / "x.db"), pseudonymize_callers=True).build_defs(None)
    with pytest.raises(ValueError, match="default_retention_days"):
        C(database_path=str(tmp_path / "x.db"), default_retention_days=-3).build_defs(None)
    comp = C(database_path=str(tmp_path / "x.db"), pseudonymize_callers=True, pseudonym_salt_env_var="RCPT_SALT_TEST")
    defs = comp.build_defs(None)
    held = defs.resources["context_receipts"]
    s = held.resource_fn(None) if callable(getattr(held, "resource_fn", None)) else held
    monkeypatch.delenv("RCPT_SALT_TEST", raising=False)
    with pytest.raises(ValueError, match="RCPT_SALT_TEST"):
        s.record_retrieval(context_name="kb", receipt_id="n1", items=[item("a")], caller_id=RAW_CALLER)
    monkeypatch.setenv("RCPT_SALT_TEST", "pepper")
    s.record_retrieval(context_name="kb", receipt_id="n1", items=[item("a")], caller_id=RAW_CALLER)
    assert s.get_receipt("n1")["caller_id"].startswith("h:")


def test_load_time_warning_for_shared_warehouse_without_retention_or_pseudonyms(mod, tmp_path, monkeypatch, caplog):
    C = mod.ContextReceiptStoreComponent
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("RCPT_SQL_URL", f"sqlite:///{tmp_path / 'w.sqlite3'}")
    kw = dict(backend="sql", sql_url_env_var="RCPT_SQL_URL", async_writes=False)
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        C(**kw).build_defs(None)
    assert any("caller ids" in r.message and "consider" in r.message for r in caplog.records)
    for extra in (dict(pseudonymize_callers=True, pseudonym_salt_env_var="X"), dict(enforce_retention=True, usage_sensor_enabled=True)):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=mod.logger.name):
            C(**kw, **extra).build_defs(None)
        assert not any("caller ids" in r.message for r in caplog.records), extra
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        C(database_path=str(tmp_path / "local.db")).build_defs(None)                 # a local SQLite file is not a shared warehouse
    assert not caplog.records
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        C(database_path=str(tmp_path / "l2.db"), default_retention_days=30).build_defs(None)
    assert any("nothing will be deleted" in r.message for r in caplog.records)
