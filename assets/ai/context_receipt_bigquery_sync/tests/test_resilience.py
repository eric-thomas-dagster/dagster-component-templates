"""Transient-failure behaviour of the BigQuery sync: retry classification, backoff, final-error shape, load-job idempotence under an
ambiguous timeout, quota accounting, and a kill-at-every-statement crash/replay matrix. Only BigQuery is faked (DuckDB-backed, executing the
real SQL); the exception classes are stand-ins NAMED like google.api_core's (the component classifies by name, never imports google)."""
import random
import sys

import pytest
from .bq_fake import FakeBigQueryClient, install_fake_bigquery

from .test_sync import DS, assert_mirrors, count, seed, sqlite_rows, state


def _exc(name, **attrs):
    return type(name, (Exception,), attrs)


ServiceUnavailable = _exc("ServiceUnavailable", code=503)
InternalServerError = _exc("InternalServerError", code=500)
TooManyRequests = _exc("TooManyRequests", code=429)
BadGateway = _exc("BadGateway", code=502)
DeadlineExceeded = _exc("DeadlineExceeded", code=504)
GatewayTimeout = _exc("GatewayTimeout", code=504)
Forbidden = _exc("Forbidden", code=403)
NotFound = _exc("NotFound", code=404)
BadRequest = _exc("BadRequest", code=400)


def forbidden(reason):
    e = Forbidden("403 quota")
    e.errors = [{"reason": reason, "message": "x"}]
    return e


class Kill(Exception):
    """The process dies here (not a transient class: nothing may retry it)."""


@pytest.fixture(autouse=True)
def _fast(comp, monkeypatch):
    rec = []
    monkeypatch.setattr(comp, "_sleep", rec.append)
    monkeypatch.setattr(comp, "_rng", random.Random(99))
    return rec


# ── classification ───────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("exc,expected", [
    (ServiceUnavailable("x"), True), (InternalServerError("x"), True), (TooManyRequests("x"), True), (BadGateway("x"), True),
    (DeadlineExceeded("x"), True), (GatewayTimeout("x"), True), (ConnectionResetError("x"), True), (TimeoutError("x"), True),
    (forbidden("rateLimitExceeded"), True), (forbidden("quotaExceeded"), True),
    (forbidden("accessDenied"), False), (Forbidden("no errors list"), False), (NotFound("x"), False), (BadRequest("x"), False),
    (ValueError("x"), False), (_exc("Unauthorized", code=401)("x"), False),
])
def test_classification(comp, exc, expected):
    assert comp._is_transient_bq(exc) is expected


@pytest.mark.parametrize("name", ["ServiceUnavailable", "InternalServerError", "TooManyRequests", "BadGateway", "GatewayTimeout", "DeadlineExceeded"])
def test_classification_by_class_name_alone_without_a_code_attribute(comp, name):
    assert comp._is_transient_bq(type(name, (Exception,), {})("x"))
    assert not comp._is_transient_bq(type("Unrelated" + name, (Exception,), {})("x"))


def test_classification_sees_subclasses_and_wrapped_causes(comp):
    class MyUnavailable(ServiceUnavailable):
        pass
    assert comp._is_transient_bq(MyUnavailable("x"))
    try:
        try:
            raise ServiceUnavailable("inner")
        except ServiceUnavailable as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert comp._is_transient_bq(outer)


# ── per-statement retry ──────────────────────────────────────────────────────────────────────────────────────────

class Flaky:
    """Wraps a FakeBigQueryClient: ``plan(n, kind, sql_or_table)`` -> None | ('before', exc) | ('after', exc) for the n-th call."""

    def __init__(self, inner, plan):
        self.inner, self.plan, self.n, self.calls = inner, plan, 0, []

    def __getattr__(self, k):
        return getattr(self.inner, k)

    def _wrap(self, kind, fn, arg, *a, **k):
        n, self.n = self.n, self.n + 1
        self.calls.append((kind, str(arg)))
        act = self.plan(n, kind, str(arg))
        if act and act[0] == "before":
            raise act[1]
        out = fn(arg, *a, **k)
        if act and act[0] == "after":
            raise act[1]
        return out

    def query(self, sql, *a, **k):
        return self._wrap("query", self.inner.query, sql, *a, **k)

    def load_table_from_json(self, rows, dest, *a, **k):
        n, self.n = self.n, self.n + 1
        self.calls.append(("load", str(dest)))
        act = self.plan(n, "load", str(dest))
        if act and act[0] == "before":
            raise act[1]
        out = self.inner.load_table_from_json(rows, dest, *a, **k)
        if act and act[0] == "after":
            raise act[1]
        return out


def fresh_bq():
    client = FakeBigQueryClient(project="proj")
    client.create_dataset("proj.ctx", exists_ok=True)
    return client, install_fake_bigquery(client)


def sync(comp, store, client, mod, **kw):
    return comp.sync_receipts(store, client, mod, dataset=DS, **kw)


@pytest.mark.parametrize("exc", [ServiceUnavailable("x"), InternalServerError("x"), TooManyRequests("x"), BadGateway("x"), DeadlineExceeded("x"),
                                 forbidden("rateLimitExceeded"), forbidden("quotaExceeded")])
def test_every_transient_class_is_retried_on_query_and_load(comp, store, bq, exc, _fast):
    client, mod = bq
    seed(store, 3)
    hits = {"query": 0, "load": 0}

    def plan(n, kind, arg):
        if kind == "query" and "DELETE FROM `proj.ctx.context_receipts`" in arg and hits["query"] < 2:
            hits["query"] += 1
            return ("before", exc)
        if kind == "load" and "context_receipt_items" in arg and hits["load"] < 2:
            hits["load"] += 1
            return ("before", exc)
    res = sync(comp, store, Flaky(client, plan), mod)
    assert hits == {"query": 2, "load": 2} and res["retries"] == 4 and len(_fast) == 4
    assert_mirrors(store, client)


def test_non_transient_error_is_not_retried(comp, store, bq, _fast):
    client, mod = bq
    seed(store, 1)
    for bad in (NotFound("nope"), BadRequest("syntax"), forbidden("accessDenied")):
        c = Flaky(client, lambda n, kind, arg, bad=bad: ("before", bad) if kind == "query" and "DELETE" in arg else None)
        with pytest.raises(type(bad)):
            sync(comp, store, c, mod)
        assert sum("DELETE" in a for _, a in c.calls) == 1 and _fast == []


def test_backoff_is_exponential_full_jitter_and_capped(comp, store, bq, _fast):
    client, mod = bq
    seed(store, 1)
    c = Flaky(client, lambda n, kind, arg: ("before", ServiceUnavailable("x")) if kind == "query" and "DELETE FROM `proj.ctx.context_receipts`" in arg else None)
    with pytest.raises(ServiceUnavailable):
        sync(comp, store, c, mod, retry_max_attempts=6, retry_max_backoff_seconds=3.0)
    assert len(_fast) == 5
    assert all(0 <= d <= min(3.0, 1.0 * 2 ** i) for i, d in enumerate(_fast)) and max(_fast) <= 3.0
    assert len(set(_fast)) == 5                                           # jittered, not a fixed ladder


def test_final_failure_raises_the_original_error_with_attempt_count(comp, store, bq):
    client, mod = bq
    seed(store, 1)
    orig = ServiceUnavailable("503 backend unavailable")
    c = Flaky(client, lambda n, kind, arg: ("before", orig) if kind == "load" and arg.endswith(".context_receipts") else None)
    with pytest.raises(ServiceUnavailable) as ei:
        sync(comp, store, c, mod, retry_max_attempts=3)
    assert ei.value is orig and orig.retry_attempts == 3
    if sys.version_info >= (3, 11):
        assert any("gave up after 3 attempt(s)" in n for n in orig.__notes__)
    assert sum(1 for k, a in c.calls if k == "load" and a.endswith(".context_receipts")) == 3
    assert state(client) == []                                             # nothing advanced


def test_retry_max_attempts_one_disables_retry(comp, store, bq):
    client, mod = bq
    seed(store, 1)
    c = Flaky(client, lambda n, kind, arg: ("before", ServiceUnavailable("x")) if kind == "query" and "DELETE" in arg else None)
    with pytest.raises(ServiceUnavailable):
        sync(comp, store, c, mod, retry_max_attempts=1)
    assert sum("DELETE" in a for _, a in c.calls) == 1


def test_a_column_probe_that_keeps_failing_transiently_is_not_mistaken_for_a_missing_column(comp, store, bq):
    client, mod = bq
    seed(store, 1)
    c = Flaky(client, lambda n, kind, arg: ("before", ServiceUnavailable("x")) if kind == "query" and "WHERE 1 = 0" in arg else None)
    with pytest.raises(ServiceUnavailable):
        sync(comp, store, c, mod, retry_max_attempts=2)
    assert not any("ALTER TABLE" in a for _, a in c.calls)


# ── ambiguous load: BigQuery accepted the job but the response never arrived ─────────────────────────────────────

@pytest.mark.parametrize("table", ["context_receipts", "context_receipt_items", "context_receipt_events", "context_sync_state"])
def test_timeout_after_the_load_was_accepted_does_not_duplicate_rows(comp, store, bq, table):
    client, mod = bq
    ids = seed(store, 3)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u")
    fired = []

    def plan(n, kind, arg):
        if kind == "load" and table in arg and not fired:
            fired.append(n)
            return ("after", DeadlineExceeded("504 deadline exceeded reading the response"))
    res = sync(comp, store, Flaky(client, plan), mod)
    assert fired and res["retries"] == 1
    assert_mirrors(store, client)                                           # exactly once, via the reused job id (409 -> get_job)
    assert len(state(client)) == 1


def test_a_failed_load_job_is_abandoned_and_the_retry_gets_a_fresh_job_id(comp, store, bq):
    client, mod = bq
    seed(store, 2)
    seen = []
    real = client.load_table_from_json

    def load(rows, dest, job_config=None, job_id=None, **kw):
        seen.append((str(dest), job_id))
        if str(dest).endswith(".context_receipts") and len([s for s in seen if s[0].endswith(".context_receipts")]) == 1:
            job = real(rows, dest, job_config=job_config, job_id=job_id)          # registers the id ...
            client.con.execute("DELETE FROM ctx.context_receipts")               # ... but the job "failed": nothing was loaded
            job.error_result = {"reason": "backendError"}
            job.result = lambda *a, **k: (_ for _ in ()).throw(InternalServerError("job failed"))
            return job
        return real(rows, dest, job_config=job_config, job_id=job_id, **kw)
    client.load_table_from_json = load
    sync(comp, store, client, mod)
    ids = [j for d, j in seen if d.endswith(".context_receipts")]
    assert len(ids) == 2 and ids[0] != ids[1]
    assert_mirrors(store, client)


# ── quota accounting ─────────────────────────────────────────────────────────────────────────────────────────────

def test_load_jobs_are_counted_per_run_and_retries_do_not_double_count(comp, store, bq):
    client, mod = bq
    ids = seed(store, 2)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u")
    fired = []
    res = sync(comp, store, Flaky(client, lambda n, k, a: (fired.append(1), ("after", DeadlineExceeded("x")))[1] if k == "load" and not fired else None), mod)
    # one batch: receipts, items, excluded, events + the state row = 5 jobs (the retried one reuses its id)
    assert res["load_jobs"] == 5 and res["batches"] == 1
    assert sync(comp, store, client, mod)["load_jobs"] == 0                  # drained: nothing to ship


def test_more_than_200_load_jobs_in_a_run_warns_with_the_quota(comp, store, bq):
    client, mod = bq
    seed(store, 60)
    msgs = []

    class L:
        def warning(self, m):
            msgs.append(m)
    res = sync(comp, store, client, mod, batch_size=1, max_batches=60, logger=L())
    assert res["load_jobs"] > 200 and len(msgs) == 1 and "1,500" in msgs[0] and str(res["load_jobs"]) in msgs[0]
    msgs.clear()
    seed(store, 2, prefix="q")
    res = sync(comp, store, client, mod, batch_size=1, max_batches=2, logger=L())
    assert res["load_jobs"] <= 200 and msgs == []


def test_asset_metadata_has_load_jobs_and_retries_and_validates_knobs(comp, store, bq):
    import dagster as dg
    from .bq_fake import FakeBigQueryResource
    from .test_sync import component
    client, _ = bq
    seed(store, 2)
    defs = component(comp).build_defs(None)
    r = dg.materialize(list(defs.assets), resources={"receipts": store, "bigquery": FakeBigQueryResource(client)})
    m = r.asset_materializations_for_node("context_receipt_bigquery_sync")[0].metadata
    assert m["load_jobs"].value == 4 and m["retries"].value == 0      # receipts, items, excluded + state (no events in this seed)
    for kw in ({"retry_max_attempts": 0}, {"retry_max_attempts": 99}, {"retry_max_backoff_seconds": 0}, {"retry_max_backoff_seconds": 1e9}):
        with pytest.raises(ValueError):
            component(comp, **kw).build_defs(None)
    c = component(comp)
    assert c.retry_max_attempts == 4 and c.retry_max_backoff_seconds == 30.0


# ── kill the run at EVERY statement position ─────────────────────────────────────────────────────────────────────

def _scenario(store):
    ids = seed(store, 5)
    store.record_outcome(ids[0], signal="thumbs_up", outcome_ref="u1")
    store.add_evidence(ids[1], level="cited", details={"n": 1})
    store.record_outcome(ids[3], signal="thumbs_down", outcome_ref="u2")
    return ids


def _final_cursors(store):
    b = store.read_since(0, 0, 10_000)
    return int(b["receipt_cursor"]), int(b["event_cursor"])


def _count_statements(comp, store):
    client, mod = fresh_bq()
    c = Flaky(client, lambda n, k, a: None)
    sync(comp, store, c, mod, batch_size=2)
    return c.n


@pytest.mark.parametrize("when", ["before", "after"])
def test_kill_at_every_statement_then_rerun_ships_everything_exactly_once(comp, store, when):
    _scenario(store)
    total = _count_statements(comp, store)
    assert total > 30                                                      # several batches x (DELETE x3, loads, probe, state ...)
    want_cursors = _final_cursors(store)
    for k in range(total):
        client, mod = fresh_bq()
        c = Flaky(client, lambda n, kind, arg, k=k: (when, Kill(f"killed at statement {k}")) if n == k else None)
        with pytest.raises(Kill):
            sync(comp, store, c, mod, batch_size=2)
        # the surviving state must already be a prefix: cursor never ahead of the data that is durably there
        rows = state(client) if k > 8 else []
        for rc, ec in rows:
            assert rc <= want_cursors[0] and ec <= want_cursors[1]
        sync(comp, store, client, mod, batch_size=2)                        # the re-run
        try:
            assert_mirrors(store, client)                                   # == the store: no duplicates, nothing missing
            assert state(client)[0] == want_cursors        # newest state row is the truth; a stale older row after a kill between insert and prune is by design
            for t in ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events"):
                assert count(client, t) == len(sqlite_rows(store, t, "1"))
            assert sync(comp, store, client, mod, batch_size=2)["load_jobs"] == 0   # and the next run is a no-op
        except AssertionError as e:
            raise AssertionError(f"kill {when} statement {k}/{total} ({c.calls[k]}): {e}") from e


def test_crash_replay_matrix_can_fail(comp, store, monkeypatch):
    """Mutation check: if the cursor were written BEFORE the batch is loaded, killing the load loses data forever -- the matrix must catch it."""
    _scenario(store)
    total = _count_statements(comp, store)
    want = _final_cursors(store)
    real_sync_batch, real_write = comp._sync_batch, comp._write_state

    def cursor_first(rt, dataset, prefix, batch):
        real_write(rt, dataset, prefix, "context_receipt_bigquery_sync", int(batch["receipt_cursor"]), int(batch["event_cursor"]))
        return real_sync_batch(rt, dataset, prefix, batch)
    monkeypatch.setattr(comp, "_sync_batch", cursor_first)
    monkeypatch.setattr(comp, "_write_state", lambda *a, **k: None)
    broken = 0
    for k in range(total):
        client, mod = fresh_bq()
        c = Flaky(client, lambda n, kind, arg, k=k: ("before", Kill("x")) if n == k else None)
        try:
            sync(comp, store, c, mod, batch_size=2)
        except Kill:
            pass
        sync(comp, store, client, mod, batch_size=2)
        try:
            assert_mirrors(store, client)
        except AssertionError:
            broken += 1
    assert broken > 0
