"""Transient-failure behaviour of the three sources, against a real local HTTP server (scripted failures), a real REAL store and real
files. Backoff sleeping is injected (recorded, never slept) so the suite is fast and deterministic."""
import json
import os
import random
import threading
import time
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import dagster as dg
import pytest

from .test_receipt_outcome_ingestion import comp, effective, md, mod, outcomes, run, smod, store, write_csv  # noqa: F401  (fixtures)

SECRET = "Bearer s3cr3t-resilience-token"
MAP = {"receipt_id": "rid", "signal": "s", "occurred_at": "t", "value_map": {"good": "thumbs_up"}, "source": "fb"}


class Flaky:
    """Serves ``respond(n, path, query)`` -> (status, headers, body-bytes); n is the 0-based request count."""

    def __init__(self, respond):
        self.respond, self.count, self.log = respond, 0, []
        flaky = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                parts = urlsplit(self.path)
                n = flaky.count
                flaky.count += 1
                flaky.log.append((parts.path, parse_qs(parts.query)))
                status, headers, body = flaky.respond(n, parts.path, parse_qs(parts.query))
                if status is None:                       # drop the connection without answering
                    self.connection.close()
                    return
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def ok(records, **extra):
    return 200, {"Content-Type": "application/json"}, json.dumps({"data": records, **extra}).encode()


@pytest.fixture
def flaky():
    made = []

    def make(respond):
        f = Flaky(respond)
        made.append(f)
        return f
    yield make
    for f in made:
        f.close()


@pytest.fixture
def sleeps(mod, monkeypatch):
    rec = []
    monkeypatch.setattr(mod, "_sleep", rec.append)
    monkeypatch.setattr(mod, "_rng", random.Random(1234))
    return rec


@pytest.fixture(autouse=True)
def _token(monkeypatch):
    monkeypatch.setenv("RES_TOKEN", SECRET)


def http_comp(mod, f, rid, **kw):
    src = {"kind": "http_json", "url": f"{f.url}/fb", "headers_env": {"Authorization": "RES_TOKEN"}, "records_path": "data"}
    src.update(kw.pop("source", {}))
    return comp(mod, src, MAP, **kw)


def rec(rid, n=1):
    return {"rid": rid, "s": "good", "t": f"2026-03-0{n}T00:00:00Z"}


def failure_text(result):
    return " ".join(str(getattr(e, "message", "")) + repr(getattr(e, "event_specific_data", "")) for e in result.all_events)


# ── http retries ─────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_status_is_retried_with_full_jitter_backoff(mod, store, flaky, sleeps, status):
    f = flaky(lambda n, p, q: (status, {}, b"") if n < 2 else ok([rec(store.rids[1])]))
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert result.success and md(result)["outcomes_recorded"] == 1 and f.count == 3
    assert len(sleeps) == 2
    assert 0 <= sleeps[0] <= 0.5 and 0 <= sleeps[1] <= 1.0           # uniform(0, base * 2**attempt)


def test_backoff_is_capped_by_max_backoff_and_jittered(mod):
    mod._rng = random.Random(7)
    ds = [mod._jitter_delay(10, 3.0) for _ in range(200)]
    assert max(ds) <= 3.0 and min(ds) < 0.5 and len(set(ds)) > 100    # capped, and actually random (not a fixed delay)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_other_4xx_never_retried_and_error_names_status_not_secret(mod, store, flaky, sleeps, status):
    f = flaky(lambda n, p, q: (status, {}, b"nope"))
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert not result.success and f.count == 1 and sleeps == []
    text = failure_text(result)
    assert f"HTTP {status}" in text and "s3cr3t-resilience-token" not in text and "RES_TOKEN" not in text


def test_retry_after_seconds_is_honoured_and_capped(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: (429, {"Retry-After": "7"}, b"") if n == 0 else ok([rec(store.rids[1])]))
    assert run(http_comp(mod, f, store.rids), store)[0].success
    assert sleeps == [7.0]
    sleeps.clear()
    f = flaky(lambda n, p, q: (503, {"Retry-After": "999"}, b"") if n == 0 else ok([rec(store.rids[1])]))
    assert run(http_comp(mod, f, store.rids, retry_max_backoff_seconds=10), store)[0].success
    assert sleeps == [10.0]


def test_retry_after_http_date(mod, store, flaky, sleeps):
    when = formatdate(time.time() + 20, usegmt=True)
    f = flaky(lambda n, p, q: (429, {"Retry-After": when}, b"") if n == 0 else ok([rec(store.rids[1])]))
    assert run(http_comp(mod, f, store.rids), store)[0].success
    assert len(sleeps) == 1 and 15 <= sleeps[0] <= 20
    assert mod._parse_retry_after("garbage") is None and mod._parse_retry_after("-5") == 0.0 and mod._parse_retry_after(None) is None
    assert mod._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0          # a date in the past: no wait


def test_exhausted_retries_report_attempt_count_and_status(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: (503, {}, b""))
    result, _ = run(http_comp(mod, f, store.rids, retry_max_attempts=3), store)
    assert not result.success and f.count == 3 and len(sleeps) == 2
    text = failure_text(result)
    assert "failed after 3 attempt(s)" in text and "HTTP 503" in text and "s3cr3t" not in text


def test_retry_max_attempts_one_disables_retrying(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: (503, {}, b""))
    assert not run(http_comp(mod, f, store.rids, retry_max_attempts=1), store)[0].success
    assert f.count == 1 and sleeps == []


def test_dropped_connection_is_retried(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: (None, {}, b"") if n < 2 else ok([rec(store.rids[1])]))
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert result.success and f.count == 3 and len(sleeps) == 2


def test_connection_refused_is_retried_then_fails_clearly(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: ok([]))
    f.close()                                       # nothing listens on that port any more
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert not result.success and len(sleeps) == 3
    assert "failed after 4 attempt(s)" in failure_text(result)


def test_request_timeout_is_applied_and_retried(mod, store, flaky, sleeps):
    def respond(n, p, q):
        if n == 0:
            time.sleep(0.8)                          # longer than timeout_seconds
        return ok([rec(store.rids[1])])
    f = flaky(respond)
    t0 = time.time()
    result, _ = run(http_comp(mod, f, store.rids, source={"timeout_seconds": 0.2}), store)
    assert result.success and f.count == 2 and len(sleeps) == 1 and time.time() - t0 < 5


def test_malformed_json_and_bad_records_path_are_clear_errors_not_retried(mod, store, flaky, sleeps):
    f = flaky(lambda n, p, q: (200, {}, b"<html>maintenance</html>"))
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert not result.success and f.count == 1 and sleeps == []
    assert "not valid JSON" in failure_text(result)
    f = flaky(lambda n, p, q: ok([rec(store.rids[1])]))
    result, _ = run(http_comp(mod, f, store.rids, source={"records_path": "nope.items"}), store)
    assert not result.success and "records_path 'nope.items' not found" in failure_text(result)
    f = flaky(lambda n, p, q: (200, {}, json.dumps({"data": "a string"}).encode()))
    result, _ = run(http_comp(mod, f, store.rids), store)
    assert not result.success and "expected a list of records" in failure_text(result)


# ── cursor invariant: a page that fails after retries fails the run and holds the cursor ─────────────────────────

def test_page_two_failing_permanently_records_nothing_and_holds_the_cursor(mod, store, flaky, sleeps):
    r = store.rids
    state = {"phase": "first", "page2_status": 200}

    def respond(n, path, q):
        page = int(q.get("page", ["1"])[0])
        if state["phase"] == "first":                                    # run 1: a healthy single record
            return ok([rec(r[1], 1)]) if page == 1 else ok([])
        if page == 1:
            return ok([rec(r[2], 2)])
        if page == 2:
            return (503, {}, b"") if state["page2_status"] != 200 else ok([rec(r[3], 3)])
        return ok([])

    f = flaky(respond)
    c = http_comp(mod, f, r, source={"page_param": "page"})
    inst = dg.DagsterInstance.ephemeral()
    m1 = md(run(c, store, inst)[0])
    cursor1 = m1["cursor"]
    assert cursor1.startswith("2026-03-01") and m1["outcomes_recorded"] == 1

    state.update(phase="second", page2_status=503)
    result, _ = run(c, store, inst)
    assert not result.success
    assert "failed after 4 attempt(s)" in failure_text(result)
    # page 1 of the failed run was NOT recorded (the read completes before anything is written), and no new cursor was written
    assert outcomes(store, r[2]) == [] and outcomes(store, r[3]) == []
    latest = inst.get_latest_materialization_event(dg.AssetKey("receipt_outcome_ingestion"))
    assert latest.asset_materialization.metadata["cursor"].value == cursor1

    state["page2_status"] = 200                                          # the API heals: next run catches everything up
    m3 = md(run(c, store, inst)[0])
    assert m3["outcomes_recorded"] >= 2 and m3["cursor"].startswith("2026-03-03")
    assert len(effective(store, r[2])) == 1 and len(effective(store, r[3])) == 1


# ── validation ────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("kw", [{"retry_max_attempts": 0}, {"retry_max_attempts": 21}, {"retry_max_backoff_seconds": 0},
                                {"retry_max_backoff_seconds": -1}, {"retry_max_backoff_seconds": 99999},
                                {"source": {"timeout_seconds": 0}}, {"source": {"timeout_seconds": "fast"}}])
def test_retry_and_timeout_fields_are_validated(mod, kw):
    source = {"kind": "http_json", "url": "http://x/y", **kw.pop("source", {})}
    with pytest.raises(ValueError):
        comp(mod, source, MAP, **kw).build_defs(None)


def test_defaults(mod):
    c = comp(mod, {"kind": "file", "path": "x"}, MAP)
    assert c.retry_max_attempts == 4 and c.retry_max_backoff_seconds == 30.0


# ── sql: transient connection errors retry (at most 3 tries), query errors do not ───────────────────────────────

class OperationalError(Exception):          # named like sqlalchemy/psycopg's; classified by name, no driver import
    pass


class ProgrammingError(Exception):
    pass


class _Client:
    def __init__(self, failures, rows):
        self.failures, self.rows, self.calls = list(failures), rows, 0

    def query(self, sql):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        import pandas as pd
        return type("J", (), {"to_dataframe": lambda s: pd.DataFrame(self.rows)})()


class _Res:
    def __init__(self, client):
        self.client = client

    def get_client(self):
        return self.client


def sql_comp(mod):
    return comp(mod, {"kind": "sql", "resource_key": "wh", "query": "SELECT * FROM t WHERE t >= {since}"}, MAP)


def test_sql_transient_errors_retry_then_succeed(mod, store, sleeps):
    cl = _Client([OperationalError("server closed the connection"), ConnectionResetError("reset")], [rec(store.rids[1])])
    result, _ = run(sql_comp(mod), store, wh=_Res(cl))
    assert result.success and cl.calls == 3 and len(sleeps) == 2 and md(result)["outcomes_recorded"] == 1


def test_sql_gives_up_after_three_tries_and_never_retries_a_bad_query(mod, store, sleeps):
    cl = _Client([OperationalError("down")] * 10, [])
    result, _ = run(sql_comp(mod), store, wh=_Res(cl))
    assert not result.success and cl.calls == 3 and "failed after 3 attempt(s)" in failure_text(result)
    sleeps.clear()
    cl = _Client([ProgrammingError("syntax error at or near FROM")], [])
    result, _ = run(sql_comp(mod), store, wh=_Res(cl))
    assert not result.success and cl.calls == 1 and sleeps == []


# ── file: corrupt jsonl lines are bad rows; a file changing under the reader is re-read ───────────────────────────

def test_jsonl_corrupt_and_half_written_lines_are_bad_rows_not_a_crash(mod, store, tmp_path):
    p = tmp_path / "fb.jsonl"
    r = store.rids
    p.write_text("\n".join([json.dumps(rec(r[1], 1)), "{not json", json.dumps([1, 2]), json.dumps(rec(r[2], 2))])
                 + '\n{"rid": "' + r[3] + '", "s": "go')                          # writer died mid-line
    c = comp(mod, {"kind": "file", "path": str(p)}, MAP)
    m = md(run(c, store)[0])
    assert m["outcomes_recorded"] == 2 and m["bad_rows"] == 3 and m["rows_read"] == 2
    assert any("invalid JSON line" in b["reason"] and str(p) in b["row"] for b in m["bad_row_examples"])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, MAP, on_bad_row="fail"), store)
    assert not result.success and "3 bad feedback row(s)" in failure_text(result)


def test_file_changing_mid_read_is_retried_then_read_consistently(mod, store, tmp_path, sleeps, monkeypatch):
    p = tmp_path / "fb.csv"
    write_csv(p, [{"rid": store.rids[1], "s": "good", "t": "2026-03-01T00:00:00Z"}])
    real = mod._read_one_file
    calls = []

    def racing(path, fmt, cfg, bad):
        out = real(path, fmt, cfg, bad)
        calls.append(1)
        if len(calls) == 1:                      # a writer touches the file while we were reading it
            os.utime(path, ns=(1, 1_700_000_000_000_000_000))
        return out

    monkeypatch.setattr(mod, "_read_one_file", racing)
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, MAP), store)
    assert result.success and len(calls) == 2 and len(sleeps) == 1 and md(result)["rows_read"] == 1

    ticks = iter(range(1, 100))                  # now a file that never settles: a clear error, not a hang or silent partial read
    monkeypatch.setattr(mod, "_read_one_file", lambda path, fmt, cfg, bad: (os.utime(path, ns=(1, next(ticks) * 10**9)), real(path, fmt, cfg, bad))[1])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, MAP), store)
    assert not result.success and "file changed while being read" in failure_text(result)


def test_corrupt_json_file_is_a_clear_error(mod, store, tmp_path):
    p = tmp_path / "fb.json"
    p.write_text('{"rows": [')
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, MAP), store)
    assert not result.success and "invalid JSON" in failure_text(result)


def test_sql_source_via_resource_key_with_the_real_dagster_gcp_resource(mod, monkeypatch):
    # required_resource_keys hands the asset dagster-gcp's BigQueryResource as a bare generator (not the resource): _run_sql must still work, repeatedly
    pytest.importorskip("dagster_gcp")
    import dagster_gcp
    import dagster_gcp.bigquery.resources as gcp_resources     # the module object dagster-gcp itself calls Client on (an earlier test may have swapped sys.modules' fake in)
    import pandas as pd

    class Job:
        def to_dataframe(self):
            return pd.DataFrame([{"receipt_id": "r1", "signal": "thumbs_up"}])

    class Client:
        queries = []

        def query(self, sql, *a, **k):
            self.queries.append(sql)
            return Job()

    client = Client()
    monkeypatch.setattr(gcp_resources.bigquery, "Client", lambda **kw: client)
    seen = []

    @dg.asset(required_resource_keys={"wh"})
    def probe(context):
        assert not hasattr(context.resources.wh, "get_client")
        for _ in range(2):
            seen.append(mod._read_sql({"query": "SELECT 1 WHERE {since} IS NOT NULL", "resource_key": "wh"}, "2026-01-01", context))

    result = dg.materialize([probe], resources={"wh": dagster_gcp.BigQueryResource(project="proj")}, raise_on_error=False)
    assert result.success, " ".join(str(e.event_specific_data) for e in result.all_events if e.is_failure)
    assert seen == [[{"receipt_id": "r1", "signal": "thumbs_up"}]] * 2 and len(client.queries) == 2
