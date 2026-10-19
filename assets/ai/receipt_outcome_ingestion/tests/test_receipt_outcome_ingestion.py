"""ReceiptOutcomeIngestionComponent against a REAL ContextReceiptStore (sqlite), real files, a real DuckDB resource and a real local
HTTP server. Nothing is mocked."""
import csv
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

import dagster as dg
import pytest

from .conftest import load_component_module, load_store_module


@pytest.fixture(scope="module")
def mod():
    return load_component_module()


@pytest.fixture(scope="module")
def smod():
    return load_store_module()


@pytest.fixture
def store(smod, tmp_path):
    s = smod.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))
    s.rids = {}
    for n in range(1, 6):  # five receipts, each serving chunk c<n> and the shared chunk "shared" of asset kb
        s.rids[n] = s.record_retrieval(
            context_name="support", dagster_asset_key="kb",
            items=[{"item_id": f"c{n}", "source_asset_key": "kb", "rank": 0}, {"item_id": "shared", "source_asset_key": "kb", "rank": 1}],
        )
    return s


def comp(mod, source, mapping, **kw):
    return mod.ReceiptOutcomeIngestionComponent(source=source, mapping=mapping, **kw)


def run(component, store, instance=None, **resources):
    instance = instance or dg.DagsterInstance.ephemeral()
    res = {"context_receipts": store, **resources}
    result = dg.materialize(component.build_defs(None).assets, resources=res, instance=instance, raise_on_error=False)
    return result, instance


def md(result):
    ev = result.get_asset_materialization_events()
    assert ev, "asset did not materialize"
    return {k: v.value for k, v in ev[0].materialization.metadata.items()}


def outcomes(store, rid):
    return [e for e in store.get_receipt(rid)["events"] if e["event_type"] == "outcome"]


def effective(store, rid):
    """Effective outcomes per ref, exactly as the store folds them (later same-ref supersedes)."""
    by_ref = {}
    for e in outcomes(store, rid):
        p = e["payload"]
        by_ref[p["outcome_ref"]] = p["value"]
    return by_ref


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def test_get_receipt_event_shape(store):
    # guard for the helpers above: tests read store events through get_receipt()
    store.record_outcome(store.rids[1], signal="thumbs_up", outcome_ref="x")
    ev = outcomes(store, store.rids[1])
    assert len(ev) == 1 and ev[0]["payload"]["outcome_ref"] == "x"


FILE_MAP = {"receipt_id": "receipt_id", "signal": "reaction", "outcome_ref": "user", "source": "slack", "occurred_at": "ts",
            "value_map": {"good": "thumbs_up", "bad": "thumbs_down"}}


def feedback_rows(store):
    r = store.rids
    return [
        {"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
        {"receipt_id": r[2], "reaction": "bad", "user": "u1", "ts": "2026-01-01T11:00:00Z"},
        {"receipt_id": r[3], "reaction": "accepted", "user": "u2", "ts": "2026-01-01T12:00:00Z"},  # passes through as a known signal
    ]


def test_csv_file_records_outcomes(mod, store, tmp_path):
    p = tmp_path / "fb.csv"
    write_csv(p, feedback_rows(store))
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    assert result.success
    m = md(result)
    assert (m["rows_read"], m["outcomes_recorded"], m["unknown_receipts"], m["bad_rows"]) == (3, 3, 0, 0)
    assert list(effective(store, store.rids[1]).values()) == [1.0]
    assert list(effective(store, store.rids[2]).values()) == [-1.0]
    assert list(effective(store, store.rids[3]).values()) == [0.5]
    assert m["cursor"].startswith("2026-01-01T12:00:00")


def test_jsonl_and_json_and_glob(mod, store, tmp_path):
    rows = feedback_rows(store)
    (tmp_path / "a.jsonl").write_text("\n".join(json.dumps(r) for r in rows[:2]) + "\n")
    (tmp_path / "b.jsonl").write_text(json.dumps(rows[2]) + "\n")
    result, _ = run(comp(mod, {"kind": "file", "path": str(tmp_path / "*.jsonl")}, FILE_MAP), store)
    assert md(result)["outcomes_recorded"] == 3
    (tmp_path / "c.json").write_text(json.dumps({"data": {"items": rows[:1]}}))
    result, _ = run(comp(mod, {"kind": "file", "path": str(tmp_path / "c.json"), "records_path": "data.items"}, FILE_MAP), store)
    assert md(result)["rows_read"] == 1


def test_parquet(mod, store, tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    p = tmp_path / "fb.parquet"
    pd.DataFrame(feedback_rows(store)).to_parquet(p)
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    assert md(result)["outcomes_recorded"] == 3


def test_missing_file_fails_loudly_unless_allow_empty(mod, store, tmp_path):
    c = comp(mod, {"kind": "file", "path": str(tmp_path / "nope-*.csv")}, FILE_MAP)
    result, _ = run(c, store)
    assert not result.success
    c = comp(mod, {"kind": "file", "path": str(tmp_path / "nope-*.csv"), "allow_empty": True}, FILE_MAP)
    result, _ = run(c, store)
    assert result.success and md(result)["rows_read"] == 0


def test_scale_and_value_map_numeric(mod, store, tmp_path):
    p = tmp_path / "csat.csv"
    r = store.rids
    write_csv(p, [{"rid": r[1], "score": "5"}, {"rid": r[2], "score": "1"}, {"rid": r[3], "score": "3"}, {"rid": r[4], "score": "4"}])
    mapping = {"receipt_id": "rid", "value": "score", "scale": {"min": 1, "max": 5}, "signal_value": "csat", "source": "csat"}
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, mapping), store)
    assert md(result)["outcomes_recorded"] == 4
    vals = [list(effective(store, r[i]).values())[0] for i in (1, 2, 3, 4)]
    assert vals == [1.0, -1.0, 0.0, 0.5]
    # numeric value_map overrides: 5 -> 1.0, 1 -> -1.0, no scale
    p2 = tmp_path / "stars.csv"
    write_csv(p2, [{"rid": r[5], "score": "5.0"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p2)}, {"receipt_id": "rid", "value": "score", "signal_value": "stars",
                                                                 "value_map": {5: 1.0, 1: -1.0}}), store)
    assert list(effective(store, r[5]).values()) == [1.0]


def test_supersession_same_ref_twice_is_one_effective_outcome(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [
        {"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
        {"receipt_id": r[1], "reaction": "bad", "user": "u1", "ts": "2026-01-01T11:00:00Z"},   # u1 changed their mind
        {"receipt_id": r[1], "reaction": "good", "user": "u2", "ts": "2026-01-01T12:00:00Z"},
    ])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    m = md(result)
    assert m["outcomes_recorded"] == 3 and m["superseded_in_run"] == 1
    assert sorted(effective(store, r[1]).values()) == [-1.0, 1.0]            # u1 -> -1 (superseded), u2 -> +1
    fb = store.item_feedback("kb")
    assert fb["c1"]["rated"] == 1 and fb["c1"]["mean_outcome"] == 0.0     # mean(-1, +1), NOT mean(+1, -1, +1)


def test_rerun_is_idempotent_for_ref_less_rows_too(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "ts": "2026-01-01T10:00:00Z"},
                  {"receipt_id": r[1], "reaction": "bad", "ts": "2026-01-01T10:05:00Z"}])
    mapping = {k: v for k, v in FILE_MAP.items() if k != "outcome_ref"}
    c = comp(mod, {"kind": "file", "path": str(p)}, mapping)
    run(c, store)
    before = effective(store, r[1])
    assert len(before) == 2
    run(c, store)   # fresh instance: no cursor, whole file re-read
    run(c, store)
    assert effective(store, r[1]) == before and store.item_feedback("kb")["c1"]["mean_outcome"] == 0.0


def test_refs_differ_across_source_labels(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"}])
    run(comp(mod, {"kind": "file", "path": str(p)}, {**FILE_MAP, "source": "slack"}), store)
    run(comp(mod, {"kind": "file", "path": str(p)}, {**FILE_MAP, "source": "email"}), store)
    assert sorted(effective(store, r[1])) == ["email:u1", "slack:u1"]


def test_unknown_receipts_and_bad_rows_are_reported_not_fatal(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    rows = [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
            {"receipt_id": "rcpt_nope", "reaction": "good", "user": "u2", "ts": "2026-01-01T10:01:00Z"},
            {"receipt_id": "rcpt_nope", "reaction": "bad", "user": "u3", "ts": "2026-01-01T10:02:00Z"},
            {"receipt_id": r[2], "reaction": "meh", "user": "u4", "ts": "2026-01-01T10:03:00Z"},          # unmapped signal
            {"receipt_id": "", "reaction": "good", "user": "u5", "ts": "2026-01-01T10:04:00Z"},           # no receipt id
            {"receipt_id": r[3], "reaction": "good", "user": "u6", "ts": "not-a-time"}]                   # bad timestamp
    rows += [{"receipt_id": r[3], "reaction": "bad", "user": f"x{i}", "ts": "2026-01-01T10:05:00Z"} for i in range(6)]
    write_csv(p, rows)
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    assert result.success
    m = md(result)
    assert m["unknown_receipts"] == 1 and m["unknown_receipt_examples"] == ["rcpt_nope"]
    assert m["bad_rows"] == 3 and len(m["bad_row_examples"]) == 3
    assert m["outcomes_recorded"] == 7 and m["rows_read"] == 12
    # first-5 cap
    p2 = tmp_path / "bad.csv"
    write_csv(p2, [{"receipt_id": "", "reaction": "good", "user": f"u{i}", "ts": "2026-01-01T10:00:00Z"} for i in range(9)])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p2)}, FILE_MAP), store)
    m = md(result)
    assert m["bad_rows"] == 9 and len(m["bad_row_examples"]) == 5


def test_on_bad_row_fail_records_nothing(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
                  {"receipt_id": r[2], "reaction": "wat", "user": "u2", "ts": "2026-01-01T10:01:00Z"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, on_bad_row="fail"), store)
    assert not result.success and effective(store, r[1]) == {}


def test_on_unknown_receipt_fail(mod, store, tmp_path):
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": "ghost", "reaction": "good", "user": "u", "ts": "2026-01-01T10:00:00Z"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, on_unknown_receipt="fail"), store)
    assert not result.success


def test_value_out_of_range_and_scale_violations_are_bad_rows(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "v.csv"
    write_csv(p, [{"rid": r[1], "v": "2"}, {"rid": r[2], "v": "abc"}, {"rid": r[3], "v": "0.25"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, {"receipt_id": "rid", "value": "v", "signal_value": "x"}), store)
    m = md(result)
    assert m["bad_rows"] == 2 and m["outcomes_recorded"] == 1
    p = tmp_path / "s.csv"
    write_csv(p, [{"rid": r[1], "v": "9"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)},
                         {"receipt_id": "rid", "value": "v", "signal_value": "x", "scale": {"min": 1, "max": 5}}), store)
    assert md(result)["bad_rows"] == 1


def test_skip_values_are_not_bad(mod, store, tmp_path):
    p = tmp_path / "z.csv"
    write_csv(p, [{"rid": store.rids[1], "s": "offered"}, {"rid": store.rids[2], "s": "good"}])
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)},
                         {"receipt_id": "rid", "signal": "s", "value_map": {"good": "thumbs_up"}, "skip_values": ["offered"]}), store)
    m = md(result)
    assert (m["skipped_rows"], m["bad_rows"], m["outcomes_recorded"]) == (1, 0, 1)


def test_incremental_cursor_second_run_reads_only_newer(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
                  {"receipt_id": r[2], "reaction": "good", "user": "u2", "ts": "2026-01-01T11:00:00Z"}])
    c = comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP)
    inst = dg.DagsterInstance.ephemeral()
    m1 = md(run(c, store, inst)[0])
    assert m1["outcomes_recorded"] == 2 and m1["rows_before_cursor"] == 0
    # nothing new: the boundary row (>= cursor, inclusive) is re-read and harmlessly superseded; the older one is not touched
    m2 = md(run(c, store, inst)[0])
    assert m2["rows_before_cursor"] == 1 and m2["cursor"] == m1["cursor"]
    assert len(effective(store, r[1])) == 1 and len(outcomes(store, r[1])) == 1       # r1 untouched: still one event
    # new feedback arrives (incl. a late one in the same second as the cursor)
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
                  {"receipt_id": r[2], "reaction": "good", "user": "u2", "ts": "2026-01-01T11:00:00Z"},
                  {"receipt_id": r[3], "reaction": "bad", "user": "u3", "ts": "2026-01-01T11:00:00Z"},
                  {"receipt_id": r[4], "reaction": "bad", "user": "u4", "ts": "2026-01-02T09:00:00Z"}])
    m3 = md(run(c, store, inst)[0])
    assert m3["rows_before_cursor"] == 1 and m3["cursor"].startswith("2026-01-02T09:00:00")
    assert list(effective(store, r[3]).values()) == [-1.0] and list(effective(store, r[4]).values()) == [-1.0]
    assert len(outcomes(store, r[2])) == 3 and len(effective(store, r[2])) == 1       # boundary row re-recorded each run (3 events), still ONE effective outcome


def test_max_rows_catches_up_over_runs(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[i], "reaction": "good", "user": f"u{i}", "ts": f"2026-01-0{i}T00:00:00Z"} for i in (1, 2, 3, 4)])
    c = comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, max_rows=2)
    inst = dg.DagsterInstance.ephemeral()
    m = md(run(c, store, inst)[0])
    assert m["outcomes_recorded"] == 2 and m["cursor"].startswith("2026-01-02")
    m = md(run(c, store, inst)[0])           # boundary row (jan 2) + jan 3
    assert m["cursor"].startswith("2026-01-03")
    run(c, store, inst)
    assert all(len(effective(store, r[i])) == 1 for i in (1, 2, 3, 4))


# ── sql: real DuckDB resource ─────────────────────────────────────────────────────────────────────────────────────

def test_sql_duckdb_with_since_placeholder(mod, store, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    from dagster_duckdb import DuckDBResource

    db = str(tmp_path / "w.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE TABLE feedback (receipt_id VARCHAR, rating INTEGER, user_id VARCHAR, created_at TIMESTAMP)")
    r = store.rids
    con.execute("INSERT INTO feedback VALUES (?, 5, 'u1', '2026-02-01 10:00:00'), (?, 1, 'u2', '2026-02-01 11:00:00')", [r[1], r[2]])
    con.close()
    c = comp(mod, {"kind": "sql", "resource_key": "warehouse",
                   "query": "SELECT receipt_id, rating, user_id, created_at FROM feedback WHERE created_at >= {since} ORDER BY created_at"},
             {"receipt_id": "receipt_id", "value": "rating", "scale": {"min": 1, "max": 5}, "signal_value": "csat",
              "outcome_ref": "user_id", "occurred_at": "created_at", "source": "csat"})
    inst = dg.DagsterInstance.ephemeral()
    res = {"warehouse": DuckDBResource(database=db)}
    m1 = md(run(c, store, inst, **res)[0])
    assert m1["outcomes_recorded"] == 2 and m1["rows_read"] == 2
    assert [list(effective(store, r[i]).values())[0] for i in (1, 2)] == [1.0, -1.0]
    con = duckdb.connect(db)
    con.execute("INSERT INTO feedback VALUES (?, 3, 'u3', '2026-02-02 09:00:00')", [r[3]])
    con.close()
    m2 = md(run(c, store, inst, **res)[0])
    assert m2["rows_read"] == 2            # SQL itself was bounded by {since} (inclusive): boundary row + the new one
    assert m2["outcomes_recorded"] == 2 and m2["cursor"].startswith("2026-02-02T09:00:00")
    assert list(effective(store, r[3]).values()) == [0.0]


def test_since_is_bound_as_an_escaped_literal(mod):
    assert mod._sql_literal("2026-01-01T00:00:00+00:00") == "'2026-01-01T00:00:00+00:00'"
    assert mod._sql_literal("x'; DROP TABLE t; --") == "'x''; DROP TABLE t; --'"


# ── http_json: real local server ──────────────────────────────────────────────────────────────────────────────────

class _Api:
    def __init__(self, pages):
        self.pages, self.requests = pages, []
        api = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                parts = urlsplit(self.path)
                q = parse_qs(parts.query)
                api.requests.append({"path": parts.path, "query": q, "headers": dict(self.headers)})
                if self.headers.get("Authorization") != "Bearer s3cr3t-token":
                    self.send_response(401)
                    self.end_headers()
                    return
                body = api.pages(parts.path, q, f"http://127.0.0.1:{api.server.server_port}")
                raw = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = HTTPServer(("127.0.0.1", 0), H)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def api_factory():
    made = []

    def make(pages):
        a = _Api(pages)
        made.append(a)
        return a
    yield make
    for a in made:
        a.close()


ZD_MAP = {"receipt_id": "external_id", "signal": "satisfaction_rating.score", "occurred_at": "updated_at", "source": "zendesk_csat",
          "outcome_ref": "id", "value_map": {"good": "thumbs_up", "good_with_comment": "thumbs_up", "bad": "thumbs_down"},
          "skip_values": ["offered", "unoffered"]}


def test_http_json_next_url_pagination_headers_env_and_secret_hygiene(mod, store, api_factory, monkeypatch):
    r = store.rids

    def pages(path, q, base):
        if q.get("page", ["1"])[0] == "2":
            return {"results": [{"id": 3, "external_id": r[3], "updated_at": "2026-03-01T12:00:00Z", "satisfaction_rating": {"score": "unoffered"}}],
                    "next_page": None}
        return {"results": [{"id": 1, "external_id": r[1], "updated_at": "2026-03-01T10:00:00Z", "satisfaction_rating": {"score": "good"}},
                            {"id": 2, "external_id": r[2], "updated_at": "2026-03-01T11:00:00Z", "satisfaction_rating": {"score": "bad"}}],
                "next_page": f"{base}/api/search?page=2"}

    api = api_factory(pages)
    monkeypatch.setenv("ZD_TOKEN_FOR_TEST", "Bearer s3cr3t-token")
    c = comp(mod, {"kind": "http_json", "url": f"{api.url}/api/search", "headers_env": {"Authorization": "ZD_TOKEN_FOR_TEST"},
                   "headers": {"X-Plain": "visible"}, "records_path": "results", "since_param": "start_time", "since_format": "epoch",
                   "next_url_path": "next_page"}, ZD_MAP)
    inst = dg.DagsterInstance.ephemeral()
    result, _ = run(c, store, inst)
    assert result.success, [e for e in result.all_events if e.is_failure]
    m = md(result)
    assert (m["rows_read"], m["outcomes_recorded"], m["skipped_rows"]) == (3, 2, 1)
    assert [x["path"] for x in api.requests] == ["/api/search", "/api/search"]
    assert api.requests[0]["query"]["start_time"] == ["0"]                       # first run: epoch of initial_since
    # second run binds the cursor as epoch seconds
    run(c, store, inst)
    assert api.requests[2]["query"]["start_time"] == [str(int(datetime(2026, 3, 1, 11, 0, tzinfo=timezone.utc).timestamp()))]  # cursor, epoch
    # the secret appears in no metadata and no event-log message
    blob = json.dumps({k: str(v) for k, v in m.items()})
    blob += " ".join(str(e.message) for e in inst.all_logs(result.run_id))
    assert "s3cr3t-token" not in blob and "ZD_TOKEN_FOR_TEST" not in blob


def test_http_json_page_param_pagination_and_max_pages(mod, store, api_factory, monkeypatch):
    r = store.rids
    served = []

    def pages(path, q, base):
        n = int(q["page"][0])
        served.append(n)
        if n > 3:
            return {"data": []}
        return {"data": [{"rid": r[n], "s": "good", "t": f"2026-03-0{n}T00:00:00Z"}]}

    api = api_factory(pages)
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    base = {"kind": "http_json", "url": f"{api.url}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data", "page_param": "page"}
    mp = {"receipt_id": "rid", "signal": "s", "occurred_at": "t", "value_map": {"good": "thumbs_up"}}
    result, _ = run(comp(mod, base, mp), store)
    assert md(result)["rows_read"] == 3 and served == [1, 2, 3, 4]               # stops at the first empty page
    served.clear()
    result, _ = run(comp(mod, {**base, "max_pages": 2}, mp), store)
    assert md(result)["rows_read"] == 2 and served == [1, 2]


def test_http_missing_env_secret_and_http_error_do_not_leak(mod, store, api_factory, monkeypatch):
    api = api_factory(lambda *a: {})
    monkeypatch.delenv("NOPE_TOKEN", raising=False)
    c = comp(mod, {"kind": "http_json", "url": f"{api.url}/x", "headers_env": {"Authorization": "NOPE_TOKEN"}}, ZD_MAP)
    result, _ = run(c, store)
    assert not result.success and api.requests == []
    monkeypatch.setenv("NOPE_TOKEN", "wrong-secret-value")
    result, inst = run(c, store)
    assert not result.success
    text = " ".join(str(getattr(e, "message", "")) + repr(getattr(e, "event_specific_data", "")) for e in result.all_events)
    assert "wrong-secret-value" not in text


# ── validation ────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("source,mapping", [
    ({"kind": "ftp"}, {"receipt_id": "a", "signal_value": "thumbs_up"}),
    ({"kind": "file", "path": "x", "bogus": 1}, {"receipt_id": "a", "signal_value": "thumbs_up"}),
    ({"kind": "file", "path": "x"}, {"signal_value": "thumbs_up"}),                                 # receipt_id is required
    ({"kind": "file", "path": "x"}, {"receipt_id": "a"}),
    ({"kind": "file", "path": "x"}, {"receipt_id": "a", "signal": "s", "signal_value": "v"}),
    ({"kind": "file", "path": "x"}, {"receipt_id": "a", "value": "v", "scale": {"min": 5, "max": 1}}),
    ({"kind": "file", "path": "x"}, {"receipt_id": "a", "signal": "s", "value_map": {"x": 7}}),
    ({"kind": "sql", "resource_key": "w"}, {"receipt_id": "a", "signal_value": "thumbs_up"}),
])
def test_invalid_config_fails_at_build_time(mod, source, mapping):
    with pytest.raises(ValueError):
        comp(mod, source, mapping).build_defs(None)


def test_standard_fields_are_wired(mod):
    c = comp(mod, {"kind": "file", "path": "x"}, {"receipt_id": "a", "signal_value": "thumbs_up"}, owners=["team:ml"], deps=["raw/feedback"],
             retry_policy_max_retries=2, cron_schedule="0 * * * *", group_name="g", asset_tags={"a": "b"})
    d = c.build_defs(None)
    spec = list(d.assets)[0]
    assert spec.group_names_by_key[dg.AssetKey("receipt_outcome_ingestion")] == "g"
    assert [s.name for s in d.schedules] == ["receipt_outcome_ingestion_schedule"]


# ── the loop closes ───────────────────────────────────────────────────────────────────────────────────────────────

def test_ingested_outcomes_reach_item_feedback_and_refresh_item_scores(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"},
                  {"receipt_id": r[2], "reaction": "bad", "user": "u2", "ts": "2026-01-01T10:01:00Z"},
                  {"receipt_id": r[3], "reaction": "bad", "user": "u3", "ts": "2026-01-01T10:02:00Z"}])
    assert store.item_feedback("kb")["c1"]["rated"] == 0
    result, _ = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    assert result.success
    fb = store.item_feedback("kb")
    assert (fb["c1"]["rated"], fb["c1"]["mean_outcome"]) == (1, 1.0)
    assert (fb["c2"]["rated"], fb["c2"]["mean_outcome"]) == (1, -1.0)
    assert fb["c4"]["rated"] == 0 and fb["c4"]["mean_outcome"] is None
    assert fb["shared"]["rated"] == 3 and fb["shared"]["mean_outcome"] == pytest.approx(-1 / 3, abs=1e-3)
    store.refresh_item_scores(prior_strength=1.0)
    sc = store.get_item_scores(["c1", "c2", "c4", "shared"])
    assert sc["c1"]["prior"] > 0 > sc["c2"]["prior"] and sc["c4"]["prior"] == 0 and sc["shared"]["prior"] < 0
    # re-running the ingestion does not move the learned priors
    run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    store.refresh_item_scores(prior_strength=1.0)
    assert store.get_item_scores(["c1", "c2", "shared"])["shared"]["prior"] == pytest.approx(sc["shared"]["prior"])


# ── review fixes: cursor safety, host pinning, tie groups, unknown-receipt grace ──────────────────────────────────

def _paged_api(api_factory, records, per_page):
    """Page-number API over ``records`` (list order is served order), 'data' + page param."""
    def pages(path, q, base):
        n = int(q.get("page", ["1"])[0])
        return {"data": records[(n - 1) * per_page: n * per_page]}
    return api_factory(pages)


def _rec(r, i, ts):
    return {"rid": r[i], "s": "good", "t": ts, "k": f"k{i}-{ts}"}


HTTP_MAP = {"receipt_id": "rid", "signal": "s", "occurred_at": "t", "value_map": {"good": "thumbs_up"}, "outcome_ref": "k"}


def test_desc_api_truncated_by_max_pages_does_not_skip_older_records(mod, store, api_factory, monkeypatch):
    r = store.rids
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    newest_first = [_rec(r, i, f"2026-03-0{i}T00:00:00Z") for i in (5, 4, 3, 2, 1)]
    api = _paged_api(api_factory, newest_first, 2)
    src = {"kind": "http_json", "url": f"{api.url}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data",
           "page_param": "page", "order": "desc", "max_pages": 1}
    inst = dg.DagsterInstance.ephemeral()
    m = md(run(comp(mod, src, HTTP_MAP), store, inst)[0])
    assert m["rows_read"] == 2 and m["read_truncated"] is True
    assert m["cursor"] == ""                                      # NOT advanced to 03-05: records 1..3 are still unread
    # a run that can reach the end picks up everything, including the old records the truncated run never saw
    m = md(run(comp(mod, {**src, "max_pages": 10}, HTTP_MAP), store, inst)[0])
    assert m["rows_read"] == 5 and m["cursor"].startswith("2026-03-05")
    assert all(len(effective(store, r[i])) == 1 for i in (1, 2, 3, 4, 5))


def test_desc_truncated_run_advances_once_it_reaches_the_previous_cursor(mod, store, api_factory, monkeypatch):
    r = store.rids
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    recs = [_rec(r, i, f"2026-03-0{i}T00:00:00Z") for i in (5, 4, 3, 2, 1)]
    api = _paged_api(api_factory, recs[3:], 2)                    # only 03-02, 03-01 exist at first
    src = {"kind": "http_json", "url": f"{api.url}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data",
           "page_param": "page", "order": "desc", "max_pages": 1}
    inst = dg.DagsterInstance.ephemeral()
    assert md(run(comp(mod, src, HTTP_MAP), store, inst)[0])["cursor"] == ""   # truncated, never reached a cursor (none yet)
    src2 = {**src, "max_pages": 10}
    assert md(run(comp(mod, src2, HTTP_MAP), store, inst)[0])["cursor"].startswith("2026-03-02")
    api.pages = lambda path, q, base: {"data": recs[(int(q.get("page", ["1"])[0]) - 1) * 2: int(q.get("page", ["1"])[0]) * 2]}
    m = md(run(comp(mod, src, HTTP_MAP), store, inst)[0])          # page 1 = 03-05, 03-04: has not reached 03-02 -> held
    assert m["read_truncated"] is True and m["cursor"].startswith("2026-03-02")
    src3 = {**src, "max_pages": 2}                                # pages 1+2 reach 03-03, 03-02 (== cursor) -> safe to advance
    m = md(run(comp(mod, src3, HTTP_MAP), store, inst)[0])
    assert m["cursor"].startswith("2026-03-05")


def test_asc_api_truncated_by_max_pages_advances_to_max_read(mod, store, api_factory, monkeypatch):
    r = store.rids
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    api = _paged_api(api_factory, [_rec(r, i, f"2026-03-0{i}T00:00:00Z") for i in (1, 2, 3, 4, 5)], 2)
    src = {"kind": "http_json", "url": f"{api.url}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data",
           "page_param": "page", "max_pages": 1}
    m = md(run(comp(mod, src, HTTP_MAP), store)[0])
    assert m["read_truncated"] is True and m["cursor"].startswith("2026-03-02")


def test_invalid_order_rejected(mod):
    with pytest.raises(ValueError):
        comp(mod, {"kind": "http_json", "url": "http://x/y", "order": "sideways"}, HTTP_MAP).build_defs(None)


def test_next_url_on_another_host_is_refused_and_no_secret_is_sent(mod, store, api_factory, monkeypatch):
    r = store.rids
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    evil = api_factory(lambda *a: {"data": []})
    good = api_factory(lambda path, q, base: {"data": [_rec(r, 1, "2026-03-01T00:00:00Z")], "next": f"{evil.url}/steal"})
    src = {"kind": "http_json", "url": f"{good.url}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data",
           "next_url_path": "next"}
    result, _ = run(comp(mod, src, HTTP_MAP), store)
    assert not result.success and evil.requests == []
    text = " ".join(str(getattr(e, "message", "")) + repr(getattr(e, "event_specific_data", "")) for e in result.all_events)
    assert "s3cr3t-token" not in text and "another host" in text


def test_cross_host_redirect_is_refused_and_no_secret_is_sent(mod, store, api_factory, monkeypatch):
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    evil = api_factory(lambda *a: {"data": []})

    class Redir(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", f"{evil.url}/steal")
            self.end_headers()

    srv = HTTPServer(("127.0.0.1", 0), Redir)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        src = {"kind": "http_json", "url": f"http://127.0.0.1:{srv.server_port}/fb", "headers_env": {"Authorization": "API_AUTH"},
               "records_path": "data"}
        result, _ = run(comp(mod, src, HTTP_MAP), store)
        assert not result.success and evil.requests == []
    finally:
        srv.shutdown()
        srv.server_close()


def test_same_host_redirect_still_works(mod, store, api_factory, monkeypatch):
    r = store.rids
    monkeypatch.setenv("API_AUTH", "Bearer s3cr3t-token")
    api = api_factory(lambda path, q, base: {"data": [_rec(r, 1, "2026-03-01T00:00:00Z")]})

    class Redir(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/real")
            self.end_headers()

    srv = HTTPServer(("127.0.0.1", 0), Redir)
    # 'real' is served by the same host: make it answer through a second handler by mapping /real onto the api via proxy-less trick
    class Both(Redir):
        def do_GET(self):
            if self.path.startswith("/real"):
                assert self.headers.get("Authorization") == "Bearer s3cr3t-token"
                raw = json.dumps({"data": [_rec(r, 1, "2026-03-01T00:00:00Z")]}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            else:
                super().do_GET()
    srv = HTTPServer(("127.0.0.1", 0), Both)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        src = {"kind": "http_json", "url": f"http://127.0.0.1:{srv.server_port}/fb", "headers_env": {"Authorization": "API_AUTH"}, "records_path": "data"}
        result, _ = run(comp(mod, src, HTTP_MAP), store)
        assert result.success and md(result)["outcomes_recorded"] == 1
    finally:
        srv.shutdown()
        srv.server_close()


def test_max_rows_never_cuts_inside_a_tie_group_and_always_progresses(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    tie = "2026-01-01T00:00:00Z"
    rows = [{"receipt_id": r[i], "reaction": "good", "user": f"u{i}", "ts": tie} for i in (1, 2, 3, 4, 5)]
    rows.append({"receipt_id": r[1], "reaction": "bad", "user": "late", "ts": "2026-01-02T00:00:00Z"})
    write_csv(p, rows)
    c = comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, max_rows=2)
    inst = dg.DagsterInstance.ephemeral()
    m1 = md(run(c, store, inst)[0])
    assert m1["outcomes_recorded"] == 5 and m1["cursor"].startswith("2026-01-01")      # whole tie group, not 2 of 5
    m2 = md(run(c, store, inst)[0])                                                     # boundary group is re-read; must still reach jan 2
    assert m2["cursor"].startswith("2026-01-02") and list(effective(store, r[1]).values()).count(-1.0) == 1


def test_unknown_receipt_within_grace_holds_cursor_and_is_retried(mod, store, tmp_path):
    r = store.rids
    now = datetime.now(timezone.utc)
    young = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    older = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_unknown = (now - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%SZ")
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": older},
                  {"receipt_id": "rcpt-not-yet-durable", "reaction": "good", "user": "u2", "ts": young},
                  {"receipt_id": "rcpt-gone", "reaction": "good", "user": "u9", "ts": old_unknown},
                  ])
    c = comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP)
    inst = dg.DagsterInstance.ephemeral()
    m1 = md(run(c, store, inst)[0])
    assert m1["unknown_receipts"] == 2 and m1["unknown_receipts_pending"] == 1
    assert _dt(m1["cursor"]) < _dt(young) and _dt(m1["cursor"]) >= _dt(older)         # held just before the young unknown row
    # the receipt becomes durable; the row is retried and recorded, the cursor moves on
    rid = store.record_retrieval(context_name="support", dagster_asset_key="kb", items=[{"item_id": "c9", "source_asset_key": "kb", "rank": 0}])
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": older},
                  {"receipt_id": rid, "reaction": "good", "user": "u2", "ts": young}])
    m2 = md(run(c, store, inst)[0])
    assert m2["unknown_receipts"] == 0 and _dt(m2["cursor"]) == _dt(young)
    assert len(effective(store, rid)) == 1


def test_unknown_receipt_grace_zero_drops_immediately(mod, store, tmp_path):
    now = datetime.now(timezone.utc)
    young = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": "nope", "reaction": "good", "user": "u", "ts": young}])
    m = md(run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, unknown_receipt_grace_hours=0), store)[0])
    assert m["unknown_receipts_pending"] == 0 and _dt(m["cursor"]) == _dt(young)


def _dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
