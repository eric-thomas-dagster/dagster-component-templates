import json

import dagster as dg
import pytest


def tools(mod, world, **kw):
    return mod.DagsterInstanceTools(world["instance"], audit=kw.pop("audit", lambda s: None), **kw)


def with_defs(mod, world, **kw):
    return tools(mod, world, definitions=world["fx"].defs, **kw)


# ---------- redaction ----------
SECRET_LINES = [
    ("Authorization: Bearer sk-live-FAKEFAKEFAKEFAKE1234567890", "FAKEFAKE"),
    ("Authorization: Basic dXNlcjpwYXNzd29yZA==", "dXNlcjpw"),
    ("curl -H 'authorization: token ghp_abcdefghijklmnopqrstuvwxyz0123456789'", "ghp_abcdef"),
    ("password: 'p@ssw0rd!'", "p@ssw0rd"),
    ("DB_PASSWORD=hunter2", "hunter2"),
    ('{"api_key": "AKIAABCDEFGHIJKLMNOP"}', "AKIAABCDEF"),
    ("postgres://svc:hunter2pass@db.internal/prod", "hunter2pass"),
    ("slack xoxb-1234567890-abcdefghij", "xoxb-1234567890"),
    ("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcDEF123", "eyJhbGci"),
    ("blob dGhpcyBpcyBhIHZlcnkgbG9uZyBiYXNlNjQgc2VjcmV0IHZhbHVlIDEyMw==", "dGhpcyBpcyBhIHZlcnkg"),
    ("client_secret = abcdef", "abcdef"),
]


@pytest.mark.parametrize("line,leak", SECRET_LINES)
def test_redact_text_masks_secret(mod, line, leak):
    out = mod.redact_text(line)
    assert leak not in out, out
    assert "[REDACTED]" in out


def test_redact_keeps_ordinary_text_and_identifiers(mod):
    s = "Materialized asset orders/daily in run 2af80c9f-5b20-4a37-9c48-107adeb3569f at /var/folders/84/tmp/storage/ok DagsterExecutionStepExecutionError"
    assert mod.redact_text(s) == s
    assert mod.redact_text("Authorization: Bearer abcdefgh12345678").startswith("Authorization: Bearer [REDACTED]")


def test_redact_obj_secret_keys_and_identifier_skip(mod):
    data = {"password": "x", "note": "token=abcSECRET123", "data_version": "a" * 31 + "1" + "b" * 32, "n": 3}
    out = mod.redact_obj(data)
    assert out["password"] == "[REDACTED]" and "abcSECRET123" not in out["note"]
    assert out["data_version"] == data["data_version"] and out["n"] == 3


# ---------- list_recent_failures / run summary / logs ----------
def test_list_recent_failures_exact(mod, world):
    r = tools(mod, world).call("list_recent_failures", {"hours": 1, "limit": 10})
    assert "error" not in r
    ids = {x["run_id"]: x for x in r["runs"]}
    assert set(ids) == {world["bad_run"], world["part_b_run"]}
    bad = ids[world["bad_run"]]
    assert bad["asset_keys"] == ["bad"]
    [step] = bad["failed_steps"]
    assert step["step_key"] == "bad" and step["error_class"] == "ValueError"
    assert step["error_message"].startswith("upstream API rejected request: HTTP 401 token=[REDACTED]")
    assert "abc123SECRETVALUE" not in json.dumps(r)
    assert "second line" not in step["error_message"]  # first line only
    assert ids[world["part_b_run"]]["failed_steps"][0]["error_class"] == "RuntimeError"


def test_list_recent_failures_window_and_limit(mod, world):
    t = tools(mod, world)
    assert t.call("list_recent_failures", {"hours": 1, "limit": 1})["count"] == 1
    assert t.call("list_recent_failures", {"hours": 0.01, "limit": 5})["count"] in (0, 2)  # 36s window
    assert "error" in t.call("list_recent_failures", {"hours": "abc"})
    assert t.call("list_recent_failures", {"limit": 10_000})["count"] == 2  # clamped, not crashed


def test_get_run_summary(mod, world):
    r = tools(mod, world).call("get_run_summary", {"run_id": world["bad_run"]})
    assert r["status"] == "FAILURE" and r["run_id"] == world["bad_run"]
    st = {s["step_key"]: s for s in r["steps"]}
    assert st["ok"]["status"] == "SUCCESS" and st["bad"]["status"] == "FAILURE"
    assert st["bad"]["asset_keys"] == ["bad"]
    assert r["failed_steps"][0]["error_class"] == "ValueError"
    assert "ValueError" in r["failed_steps"][0]["error_detail"]
    assert "abc123SECRETVALUE" not in json.dumps(r)
    assert r["duration_s"] is not None and r["started_at"] and r["ended_at"]


def test_get_run_logs_redacts_and_filters(mod, world):
    t = tools(mod, world)
    r = t.call("get_run_logs", {"run_id": world["bad_run"], "level": "INFO", "max_lines": 500})
    text = "\n".join(r["lines"])
    for leak in ("FAKEFAKEFAKE", "hunter2pass", "AKIAABCDEFGHIJKLMNOP", "p@ssw0rd"):
        assert leak not in text, leak
    assert "Authorization: Bearer [REDACTED]" in text
    assert "postgres://svc:[REDACTED]@db.internal/prod" in text
    # level filter: only WARNING+ -> the warning line (and error events), not the INFO bearer line
    w = t.call("get_run_logs", {"run_id": world["bad_run"], "level": "WARNING", "max_lines": 500})
    assert any("retrying" in l for l in w["lines"]) and not any("Bearer" in l for l in w["lines"])
    assert w["total_matching"] < r["total_matching"]


def test_get_run_logs_tail_cap(mod, world):
    r = tools(mod, world).call("get_run_logs", {"run_id": world["bad_run"], "level": "DEBUG", "max_lines": 3})
    assert r["returned"] == 3 and len(r["lines"]) == 3 and r["total_matching"] > 3
    full = tools(mod, world).call("get_run_logs", {"run_id": world["bad_run"], "level": "DEBUG", "max_lines": 500})
    assert r["lines"] == full["lines"][-3:]  # tail, not head


def test_bad_ids_give_clear_errors(mod, world):
    t = tools(mod, world)
    assert "run not found" in t.call("get_run_summary", {"run_id": "nope"})["error"]
    assert "run not found" in t.call("get_run_logs", {"run_id": "nope"})["error"]
    assert "run_id is required" in t.call("get_run_summary", {})["error"] or "error" in t.call("get_run_summary", {})
    assert "level must be" in t.call("get_run_logs", {"run_id": world["bad_run"], "level": "LOUD"})["error"]
    assert "asset_key is required" in t.call("asset_status", {"asset_key": ""})["error"]
    assert "empty path segment" in t.call("asset_status", {"asset_key": "a//b"})["error"]
    assert "bad arguments" in t.call("asset_status", {"nope": 1})["error"]
    assert "unknown tool" in t.call("drop_database", {})["error"]
    assert t.call("asset_status", {"asset_key": "never_heard_of_it"})["found"] is False


# ---------- asset_status ----------
def test_asset_status_ok(mod, world):
    r = tools(mod, world).call("asset_status", {"asset_key": "ok"})
    lm = r["last_materialization"]
    assert r["found"] and lm["run_id"] == world["chk_run"]  # newest materialization of ok
    assert len(lm["data_version"]) == 64 and lm["code_version"] == world["chk_run"]
    assert r["latest_failed_run_since"] is None and r["partitions"] is None and r["checks"] == []


def test_asset_status_failing_asset_reports_failed_run(mod, world):
    r = tools(mod, world).call("asset_status", {"asset_key": "bad"})
    assert r["found"] and r["last_materialization"] is None
    assert r["latest_failed_run_since"]["run_id"] == world["bad_run"]
    assert r["latest_failed_run_since"]["failed_steps"][0]["error_class"] == "ValueError"


def test_asset_status_check_results(mod, world):
    r = tools(mod, world).call("asset_status", {"asset_key": "chk"})
    [c] = r["checks"]
    assert c["check"] == "row_count_positive" and c["status"] == "FAILED" and c["passed"] is False
    assert c["description"] == "0 rows" and c["run_id"] == world["chk_run"]


def test_asset_status_partitions_with_defs(mod, world):
    r = with_defs(mod, world).call("asset_status", {"asset_key": "part"})
    p = r["partitions"]
    assert (p["total"], p["materialized"], p["failed"], p["missing"]) == (3, 1, 1, 1)
    assert p["failed_sample"] == ["b"] and p["missing_sample"] == ["c"]


def test_asset_status_partitions_without_defs_is_honest(mod, world):
    p = tools(mod, world).call("asset_status", {"asset_key": "part"})["partitions"]
    assert p["materialized"] == 1 and p["materialized_sample"] == ["a"]
    assert p["missing"] is None and p["failed"] is None and "unavailable" in p["note"]


def test_asset_status_not_in_defs_flag(mod, world):
    r = with_defs(mod, world).call("asset_status", {"asset_key": "ok"})
    assert r["in_loaded_definitions"] is True


# ---------- lineage ----------
def test_lineage_with_defs_both_directions(mod, world):
    t = with_defs(mod, world)
    up = t.call("asset_lineage", {"asset_key": "chk", "direction": "upstream"})
    assert up["source"] == "asset_graph" and up["upstream"] == ["ok"] and up["downstream"] == []
    down = t.call("asset_lineage", {"asset_key": "ok", "direction": "downstream", "depth": 1})
    assert down["downstream"] == ["bad", "chk"]
    assert {"from": "ok", "to": "bad"} in down["edges"]
    assert "error" in t.call("asset_lineage", {"asset_key": "ghost"})
    assert "direction must be" in t.call("asset_lineage", {"asset_key": "ok", "direction": "sideways"})["error"]


def test_lineage_event_log_fallback_upstream_only(mod, world):
    t = tools(mod, world)
    up = t.call("asset_lineage", {"asset_key": "chk"})
    assert up["source"] == "event_log_provenance" and up["upstream"] == ["ok"] and up["downstream"] is None
    err = t.call("asset_lineage", {"asset_key": "ok", "direction": "downstream"})["error"]
    assert "definitions_module" in err


# ---------- stale ----------
def test_stale_assets_age_based_without_defs(mod, world, monkeypatch):
    r = tools(mod, world).call("stale_assets", {"limit": 10, "max_age_hours": 24})
    assert r["source"] == "age_based"
    # nothing is older than 24h, but `bad` (never succeeded) and `part` (partition b failed after a) have later failed runs
    assert {a["asset_key"] for a in r["assets"]} == {"bad", "part"}  # failed runs after their last good materialization
    assert all(a["reason"] == "latest run failed after last materialization" for a in r["assets"])
    real = mod.time.time()
    monkeypatch.setattr(mod.time, "time", lambda: real + 2 * 3600)  # two hours later
    r0 = tools(mod, world).call("stale_assets", {"limit": 10, "max_age_hours": 1})
    by = {a["asset_key"]: a for a in r0["assets"]}
    assert set(by) == {"ok", "chk", "part", "bad"}
    assert 1.9 < by["ok"]["age_hours"] < 2.2 and by["ok"]["reason"] == "older than 1h"
    assert r0["assets"][0]["asset_key"] == "bad"  # never-materialized sorts first


def test_stale_assets_data_version_with_defs(mod, world):
    import time
    inst = world["instance"]
    fx = world["fx"]
    # re-materialize upstream `ok` only -> `chk` (built from the old ok) becomes STALE by data version
    dg.materialize([fx.ok], instance=inst)
    r = with_defs(mod, world).call("stale_assets", {"limit": 10})
    assert r["source"] == "data_version_staleness"
    keys = {a["asset_key"]: a for a in r["assets"]}
    assert "chk" in keys and keys["chk"]["status"] == "STALE"
    assert keys["chk"]["causes"], "should say why"
    # refresh chk -> no longer stale
    dg.materialize([fx.ok, fx.chk], instance=inst)
    r2 = with_defs(mod, world).call("stale_assets", {"limit": 10})
    assert "chk" not in {a["asset_key"] for a in r2["assets"]}


# ---------- receipts ----------
class FakeStore:
    def __init__(self):
        self.calls = []

    def asset_usage_since(self, key, since):
        self.calls.append(("usage", key, since))
        return {"receipts": 3, "items": [("p1", "v1")] * 100, "mean_outcome": 0.5}

    def outcomes_by_code_version(self, key, since=""):
        self.calls.append(("outcomes", key, since))
        return {"v1": {"receipts": 2, "outcomes": 2, "mean_outcome": 0.25}}


def test_receipt_stats(mod, world):
    st = FakeStore()
    r = tools(mod, world, receipt_store=st).call("receipt_stats", {"asset_key": "ok", "window_hours": 24})
    assert r["outcomes_by_code_version"]["v1"]["mean_outcome"] == 0.25 and r["usage"]["receipts"] == 3
    assert len(r["usage"]["items"]) <= 30
    assert st.calls[0][1] == "ok" and st.calls[0][2] == st.calls[1][2]
    assert "not configured" in tools(mod, world).call("receipt_stats", {"asset_key": "ok"})["error"]


# ---------- allow list / caps / audit ----------
def test_allow_list_enforced(mod, world):
    t = tools(mod, world, allow=["asset_status"])
    assert t.tool_names() == ["asset_status"]
    assert t.call("asset_status", {"asset_key": "ok"})["found"]
    err = t.call("get_run_logs", {"run_id": world["bad_run"]})
    assert "not enabled" in err["error"] and "lines" not in err
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DagsterInstanceTools(world["instance"], allow=["asset_status", "delete_run"])


def test_max_result_chars_cap(mod, world):
    t = tools(mod, world, max_result_chars=700)
    r = t.call("get_run_logs", {"run_id": world["bad_run"], "level": "DEBUG", "max_lines": 500})
    assert len(json.dumps(r)) <= 700 and r.get("truncated") is True
    big = tools(mod, world, max_result_chars=200)
    r2 = big.call("get_run_summary", {"run_id": world["bad_run"]})
    assert len(json.dumps(r2)) <= 200 + 50 and r2["truncated"] is True


def test_audit_line_has_tool_and_args_not_results(mod, world):
    lines = []
    t = tools(mod, world, audit=lines.append)
    t.call("asset_status", {"asset_key": "ok"})
    t.call("get_run_logs", {"run_id": world["bad_run"], "level": "INFO"})
    t.call("asset_status", {"asset_key": "token=SUPERSECRETVALUE1"})
    assert len(lines) == 3
    assert "tool=asset_status" in lines[0] and '"asset_key": "ok"' in lines[0]
    assert world["bad_run"] in lines[1]
    assert "last_materialization" not in lines[0] and "Bearer" not in lines[1]
    assert "SUPERSECRETVALUE1" not in lines[2]


def test_audit_failure_never_breaks_call(mod, world):
    def boom(_):
        raise RuntimeError("sink down")
    assert mod.DagsterInstanceTools(world["instance"], audit=boom).call("asset_status", {"asset_key": "ok"})["found"]


def test_read_only_no_mutation(mod, world):
    inst = world["instance"]
    before = len(inst.get_runs()), len(inst.all_logs(world["bad_run"]))
    t = tools(mod, world, definitions=world["fx"].defs)
    for tool, a in [("list_recent_failures", {}), ("get_run_summary", {"run_id": world["bad_run"]}),
                    ("get_run_logs", {"run_id": world["bad_run"]}), ("asset_status", {"asset_key": "part"}),
                    ("asset_lineage", {"asset_key": "ok"}), ("stale_assets", {})]:
        t.call(tool, a)
    assert before == (len(inst.get_runs()), len(inst.all_logs(world["bad_run"])))


# ---------- component / resource ----------
def test_component_registers_resource_and_validates_allow(mod, world, tmp_path):
    comp = mod.DagsterInstanceToolsComponent(allow=["asset_status", "get_run_summary"], max_result_chars=3000)
    defs = comp.build_defs(None)
    assert "dagster_instance_tools" in defs.resources
    @dg.asset(required_resource_keys={"dagster_instance_tools"})
    def probe(context):
        t = context.resources.dagster_instance_tools
        assert t.tool_names() == ["get_run_summary", "asset_status"]  # canonical order
        r = t.call("asset_status", {"asset_key": "ok"})
        assert r["found"]
        context.add_output_metadata({"names": ",".join(t.tool_names())})
    res = dg.materialize([probe], resources=defs.resources, instance=world["instance"])
    assert res.success
    with pytest.raises(ValueError, match="unknown tool"):
        mod.DagsterInstanceToolsComponent(allow=["nope"]).build_defs(None)
    with pytest.raises(ValueError, match="store_resource_key"):
        mod.DagsterInstanceToolsComponent(allow=["receipt_stats"]).build_defs(None)
    # default allow without a store drops receipt_stats
    d2 = mod.DagsterInstanceToolsComponent().build_defs(None)
    assert "dagster_instance_tools" in d2.resources


def test_component_store_resource_key_enables_receipt_stats(mod, world):
    defs = mod.DagsterInstanceToolsComponent(store_resource_key="receipts").build_defs(None)
    st = FakeStore()
    @dg.asset(required_resource_keys={"dagster_instance_tools"})
    def probe(context):
        assert "receipt_stats" in context.resources.dagster_instance_tools.tool_names()
        assert context.resources.dagster_instance_tools.call("receipt_stats", {"asset_key": "ok"})["usage"]["receipts"] == 3
    res = dg.materialize([probe], resources={**defs.resources, "receipts": st}, instance=world["instance"])
    assert res.success


def test_final_deep_redaction_is_defense_in_depth(mod, world):
    class Leaky(mod.DagsterInstanceTools):
        def t_asset_status(self, asset_key=None):
            return {"note": "Authorization: Bearer abcdefgh12345678", "password": "pw", "data_version": "a1" * 32}
    r = Leaky(world["instance"], audit=lambda s: None).call("asset_status", {"asset_key": "x"})
    assert "abcdefgh12345678" not in json.dumps(r) and r["password"] == "[REDACTED]"
    assert r["data_version"] == "a1" * 32  # identifiers survive
