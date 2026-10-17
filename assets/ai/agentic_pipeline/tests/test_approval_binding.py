"""An approval is bound to WHAT was approved: fingerprint match, reuse (once | until_changed | always), max_age_hours,
and the reason shown when a token exists but is not honoured. Real runs of the approve op on a local directory and on
fsspec memory://; the clock is faked only by writing old `approved_at` / file mtimes (no sleeping)."""
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone

import dagster as dg
import fsspec
import pytest

from .conftest import new_state


class Dir:
    """A local or memory:// approval dir, read/written directly (not through the code under test)."""

    def __init__(self, kind, tmp_path):
        self.kind = kind
        if kind == "local":
            self.uri, self.root = str(tmp_path / "ap"), tmp_path / "ap"
        else:
            self.root = "/ap-" + uuid.uuid4().hex
            self.uri, self.fs = "memory:/" + self.root, fsspec.filesystem("memory")

    def read(self, *parts):
        rel = "/".join(parts)
        return json.loads(self.fs.cat_file(f"{self.root}/{rel}") if self.kind == "memory" else (self.root / rel).read_text())

    def write(self, name, obj):
        if self.kind == "memory":
            self.fs.pipe_file(f"{self.root}/{name}", json.dumps(obj).encode())
        else:
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / name).write_text(json.dumps(obj))

    def exists(self, *parts):
        rel = "/".join(parts)
        return self.fs.exists(f"{self.root}/{rel}") if self.kind == "memory" else (self.root / rel).exists()


@pytest.fixture(params=["local", "memory"])
def d(request, tmp_path):
    return Dir(request.param, tmp_path)


def step(d, **kw):
    return {"id": "gate", "op": "approve", "source": "draft", "approval_dir": d.uri, "key": "k1", "on_timeout": "pending", **kw}


def run(mod, ctx, st, text="Refund $120 to customer 9"):
    state = new_state()
    state["draft"] = {"text": text}
    mod._run_step(st, state, ctx)
    return state["gate"]


def ago(**kw):
    return (datetime.now(timezone.utc) - timedelta(**kw)).isoformat()


def test_approving_the_pending_record_as_written_passes_and_is_reused_while_input_is_unchanged(mod, ctx, d):
    assert run(mod, ctx, step(d))["status"] == "pending"
    pend = d.read("pending", "k1.json")
    assert pend["state"] == "new" and pend["reason"] is None and pend["fingerprint"]
    tok = mod.approve_pending(d.uri, "k1", approver="eric", reason="ok")
    assert tok["fingerprint"] == pend["fingerprint"]
    for _ in range(3):                                           # same input: every later run passes, token untouched
        out = run(mod, ctx, step(d))
        assert out["approved"] is True and out["status"] == "approved" and out["approver"] == "eric"
    assert "consumed_at" not in d.read("k1.json")
    assert not d.exists("pending", "k1.json")


def test_changed_input_rerequests_with_the_reason_in_log_and_pending_record(mod, ctx, d, caplog):
    run(mod, ctx, step(d))
    mod.approve_pending(d.uri, "k1", approver="eric")
    with caplog.at_level(logging.WARNING, logger="agentic_test"):
        out = run(mod, ctx, step(d), text="Refund $9,000 to customer 9")
    assert out["status"] == "pending" and out["approved"] is None
    assert out["approval_not_honoured_reason"] == "input changed since approval"
    assert any("input changed since approval" in r.getMessage() for r in caplog.records)
    pend = d.read("pending", "k1.json")
    assert pend["state"] == "stale" and pend["reason"] == "input changed since approval"
    assert "9,000" in pend["preview"]
    # the old yes does not bleed through; approving the NEW pending record as written lets the new input pass
    assert run(mod, ctx, step(d), text="Refund $9,000 to customer 9")["status"] == "pending"
    mod.approve_pending(d.uri, "k1", approver="eric")
    assert run(mod, ctx, step(d), text="Refund $9,000 to customer 9")["status"] == "approved"
    assert run(mod, ctx, step(d))["status"] == "pending"          # and reverting the input is a change again


def test_stale_rejection_does_not_block_a_changed_input_either(mod, ctx, d):
    run(mod, ctx, step(d))
    mod.approve_pending(d.uri, "k1", approved=False, reason="no")
    with pytest.raises(RuntimeError, match="no"):
        run(mod, ctx, step(d, on_timeout="reject"))
    assert run(mod, ctx, step(d), text="something else")["status"] == "pending"


def test_a_token_without_a_fingerprint_is_not_bound_and_is_not_honoured(mod, ctx, d):
    d.write("k1.json", {"approved": True, "approver": "hand-written"})
    out = run(mod, ctx, step(d))
    assert out["status"] == "pending" and "no fingerprint" in out["approval_not_honoured_reason"]
    assert d.read("pending", "k1.json")["state"] == "stale"


def test_once_is_consumed_when_a_run_passes_and_the_next_run_needs_a_fresh_approval(mod, ctx, d):
    st = step(d, reuse="once")
    run(mod, ctx, st)
    mod.approve_pending(d.uri, "k1", approver="eric")
    assert run(mod, ctx, st)["status"] == "approved"
    assert d.read("k1.json")["consumed_at"]                       # kept as the audit record, marked used
    out = run(mod, ctx, st)                                       # SAME input, but the approval was spent
    assert out["status"] == "pending" and out["approval_not_honoured_reason"].startswith("consumed")
    assert d.read("pending", "k1.json")["state"] == "consumed"
    mod.approve_pending(d.uri, "k1", approver="eric")             # a fresh approval overwrites the spent one
    assert run(mod, ctx, st)["status"] == "approved"


def test_until_changed_honours_a_token_that_an_earlier_once_run_consumed_only_if_fresh(mod, ctx, d):
    run(mod, ctx, step(d))
    mod.approve_pending(d.uri, "k1")
    run(mod, ctx, step(d, reuse="once"))
    assert run(mod, ctx, step(d))["status"] == "pending"          # consumed is consumed, whatever the current reuse mode


def test_once_never_marks_a_token_the_reviewer_wrote_after_the_run_read_it(mod, ctx, d, monkeypatch):
    st = step(d, reuse="once")
    run(mod, ctx, st)
    old = mod.approve_pending(d.uri, "k1", approver="old")
    real = mod._ApprovalStore.remove

    def remove_then_reviewer_writes(self, p):                     # fires between the token read and the consume step
        real(self, p)
        d.write("k1.json", {**old, "approver": "newer", "approved_at": ago(seconds=-1)})
    monkeypatch.setattr(mod._ApprovalStore, "remove", remove_then_reviewer_writes)
    assert run(mod, ctx, st)["approver"] == "old"
    assert "consumed_at" not in d.read("k1.json") and d.read("k1.json")["approver"] == "newer"


def test_max_age_hours_expires_an_old_token_and_says_so(mod, ctx, d):
    run(mod, ctx, step(d, max_age_hours=24))
    tok = mod.approve_pending(d.uri, "k1", approver="eric")
    assert run(mod, ctx, step(d, max_age_hours=24))["status"] == "approved"                 # fresh
    d.write("k1.json", {**tok, "approved_at": ago(hours=30)})
    out = run(mod, ctx, step(d, max_age_hours=24))
    assert out["status"] == "pending" and out["approval_not_honoured_reason"].startswith("approval expired")
    assert d.read("pending", "k1.json")["state"] == "expired"
    assert run(mod, ctx, step(d))["status"] == "approved"                                    # no max_age_hours: no expiry


def test_token_age_falls_back_to_file_mtime_locally(mod, ctx, tmp_path):
    d = Dir("local", tmp_path)
    run(mod, ctx, step(d, max_age_hours=1))
    tok = mod.approve_pending(d.uri, "k1")
    d.write("k1.json", {k: v for k, v in tok.items() if k != "approved_at"})
    assert run(mod, ctx, step(d, max_age_hours=1))["status"] == "approved"
    old = (datetime.now() - timedelta(hours=5)).timestamp()
    os.utime(d.root / "k1.json", (old, old))
    assert run(mod, ctx, step(d, max_age_hours=1))["approval_not_honoured_reason"].startswith("approval expired")


def test_always_is_the_explicit_opt_in_to_the_old_behaviour(mod, ctx, d):
    d.write("k1.json", {"approved": True, "approver": "eric"})                # no fingerprint needed
    for text in ("one input", "a completely different input"):
        assert run(mod, ctx, step(d, reuse="always"), text=text)["status"] == "approved"
    d.write("k1.json", {"approved": True, "approver": "eric", "approved_at": ago(days=5)})
    assert run(mod, ctx, step(d, reuse="always", max_age_hours=24))["status"] == "pending"   # expiry still applies


def test_bind_to_summary_ignores_text_that_a_model_regenerates_every_run(mod, ctx, d):
    st = step(d, summary="Pay invoice 7?", bind_to=["summary"])
    run(mod, ctx, st, text="draft v1")
    mod.approve_pending(d.uri, "k1")
    assert run(mod, ctx, st, text="draft v2 (re-generated)")["status"] == "approved"
    assert run(mod, ctx, {**st, "summary": "Pay invoice 8?"}, text="draft v2")["status"] == "pending"


def test_token_arriving_during_the_wait_must_match_too(mod, ctx, d):
    def reviewer():
        import time
        time.sleep(0.3)
        d.write("k1.json", {"approved": True, "approver": "wrong", "fingerprint": "deadbeef"})   # bound to something else
        time.sleep(0.3)
        mod.approve_pending(d.uri, "k1", approver="right")
    t = threading.Thread(target=reviewer)
    t.start()
    out = run(mod, ctx, step(d, wait_seconds=5, poll_seconds=0.1))
    t.join()
    assert out["status"] == "approved" and out["approver"] == "right"


def test_bad_options_are_rejected_at_load(mod, tmp_path):
    def build(**kw):
        return mod.AgenticPipelineComponent(
            asset_name_prefix="p", source={"kind": "literal", "text": "t"},
            steps=[{"id": "g", "op": "approve", "approval_dir": str(tmp_path), **kw}], outputs={"assets": ["g"]}).build_defs(None)
    with pytest.raises(ValueError, match="reuse must be one of"):
        build(reuse="forever")
    with pytest.raises(ValueError, match="max_age_hours must be a number > 0"):
        build(max_age_hours=0)
    with pytest.raises(ValueError, match="bind_to"):
        build(bind_to=["nope"])
    build(reuse="once", max_age_hours=12, bind_to=["summary"])


def test_two_partitions_consume_and_expire_independently(mod, fake_litellm, tmp_path, monkeypatch):
    """Real partitioned runs: per-key token files, so consuming / expiring one partition never touches another."""
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    ap = tmp_path / "ap"
    c = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "doc for {partition_key}"},
        steps=[{"id": "gate", "op": "approve", "approval_dir": str(ap), "reuse": "once"}], outputs={"assets": ["gate"]},
        partition_type="static", partition_values="a,b")
    defs = c.build_defs(None)

    def go(k):
        return dg.materialize(defs.assets, partition_key=k, raise_on_error=False).success
    assert not go("a") and not go("b")
    mod.approve_pending(str(ap), "a")
    mod.approve_pending(str(ap), "b")
    results = {}
    threads = [threading.Thread(target=lambda k=k: results.__setitem__(k, go(k))) for k in ("a", "b")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results == {"a": True, "b": True}
    assert json.loads((ap / "a.json").read_text())["consumed_at"] and json.loads((ap / "b.json").read_text())["consumed_at"]
    assert not go("a")                                            # a's approval is spent ...
    mod.approve_pending(str(ap), "a")
    assert go("a")                                                # ... and a fresh one for a does not matter to b
    assert json.loads((ap / "a.json").read_text()).get("consumed_at") and not go("b")
