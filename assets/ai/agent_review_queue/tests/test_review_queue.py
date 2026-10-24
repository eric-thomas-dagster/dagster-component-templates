"""AgentReviewQueueComponent: reads what agentic_pipeline's `approve` op writes."""
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_agentic_module, load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def agentic():
    return load_agentic_module()


def ask(agentic, approval_dir, key, summary, **extra):
    """Open a real review request through the real approve op (no token, on_timeout=pending => it stays in the queue)."""
    state = {"source": {"text": "payload"}, "draft": {"text": summary}, "__ctx__": {"partition_key": None, "partition_fields": None}}
    step = {"id": f"gate_{key}", "op": "approve", "source": "draft", "approval_dir": str(approval_dir), "key": key,
            "summary": summary, "on_timeout": "pending", **extra}
    agentic._run_step(step, state, SimpleNamespace(log=logging.getLogger("t")))
    return state[step["id"]]


def component(mod, dirs, **kw):
    return mod.AgentReviewQueueComponent(approval_dirs=[str(d) for d in dirs], **kw)


def materialize(comp):
    return dg.materialize(comp.build_defs(None).assets, raise_on_error=False)


def queue_df(result, name="agent_review_queue"):
    return result.output_for_node(name)


def test_empty_queue_is_an_empty_frame_and_the_check_passes(mod, tmp_path):
    r = materialize(component(mod, [tmp_path]))
    assert r.success and len(queue_df(r)) == 0
    assert [e.passed for e in r.get_asset_check_evaluations()] == [True]


def test_lists_real_pending_reviews_across_directories(mod, agentic, tmp_path):
    a, b = tmp_path / "billing", tmp_path / "support"
    ask(agentic, a, "INV-1", "Pay $4,180 to Acme?")
    ask(agentic, a, "INV-2", "Pay $90 to Globex?")
    ask(agentic, b, "REF-7", "Refund $120?")
    r = materialize(component(mod, [a, b]))
    df = queue_df(r)
    assert sorted(df["key"]) == ["INV-1", "INV-2", "REF-7"]
    row = df[df["key"] == "INV-1"].iloc[0]
    assert row["step"] == "gate_INV-1" and row["summary"] == "Pay $4,180 to Acme?" and row["on_timeout"] == "pending"
    assert row["approve_path"].endswith("INV-1.json")                                     # tells the reviewer exactly what to write
    assert not df["overdue"].any()
    md = r.get_asset_materialization_events()[0].materialization.metadata
    assert md["pending_count"].value == 3 and md["overdue_count"].value == 0


def test_a_decided_review_leaves_the_queue(mod, agentic, tmp_path):
    ask(agentic, tmp_path, "INV-1", "Pay?")
    ask(agentic, tmp_path, "INV-2", "Pay?")
    agentic.approve_pending(str(tmp_path), "INV-1", approver="eric")  # approve the pending record as written
    ask(agentic, tmp_path, "INV-1", "Pay?")                          # the gate re-runs, sees the token, and clears its queue entry
    assert list(queue_df(materialize(component(mod, [tmp_path])))["key"]) == ["INV-2"]


def test_queue_shows_why_approval_was_requested_again(mod, agentic, tmp_path):
    ask(agentic, tmp_path, "NEW", "first request")
    ask(agentic, tmp_path, "CHG", "Pay $10?")
    agentic.approve_pending(str(tmp_path), "CHG")
    ask(agentic, tmp_path, "CHG", "Pay $9,000?")                      # summary changed after approval -> stale
    ask(agentic, tmp_path, "OLD", "Pay?", max_age_hours=1)
    tok = agentic.approve_pending(str(tmp_path), "OLD")
    (tmp_path / "OLD.json").write_text(json.dumps({**tok, "approved_at": (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()}))
    ask(agentic, tmp_path, "OLD", "Pay?", max_age_hours=1)            # token older than max_age_hours -> expired
    ask(agentic, tmp_path, "ONCE", "Pay?", reuse="once")
    agentic.approve_pending(str(tmp_path), "ONCE")
    ask(agentic, tmp_path, "ONCE", "Pay?", reuse="once")              # passes and spends the approval
    ask(agentic, tmp_path, "ONCE", "Pay?", reuse="once")              # same input again -> consumed
    r = materialize(component(mod, [tmp_path]))
    df = queue_df(r).set_index("key")
    assert df.loc["NEW", "state"] == "new" and pd.isna(df.loc["NEW", "reason"])
    assert df.loc["CHG", "state"] == "stale" and df.loc["CHG", "reason"] == "input changed since approval"
    assert df.loc["OLD", "state"] == "expired" and df.loc["OLD", "reason"].startswith("approval expired")
    assert df.loc["ONCE", "state"] == "consumed" and df.loc["ONCE", "reason"].startswith("consumed")
    assert r.get_asset_materialization_events()[0].materialization.metadata["re_requested_count"].value == 3


def write_pending(tmp_path, key, requested_at, expires_at=None):
    (tmp_path / "pending").mkdir(exist_ok=True)
    (tmp_path / "pending" / f"{key}.json").write_text(json.dumps({
        "step": "gate", "key": key, "requested_at": requested_at.isoformat(),
        "expires_at": expires_at.isoformat() if expires_at else None, "on_timeout": "reject", "summary": f"review {key}"}))


def test_overdue_by_age_flags_the_check_as_a_warning(mod, tmp_path):
    now = datetime.now(timezone.utc)
    write_pending(tmp_path, "old", now - timedelta(hours=3))
    write_pending(tmp_path, "fresh", now - timedelta(minutes=5))
    r = materialize(component(mod, [tmp_path], overdue_after_minutes=60))
    df = queue_df(r).set_index("key")
    assert bool(df.loc["old", "overdue"]) and not bool(df.loc["fresh", "overdue"]) and df.loc["old", "age_minutes"] > 170
    (ev,) = r.get_asset_check_evaluations()
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["overdue_keys"].value == ["old"]


def test_a_review_with_a_future_expiry_is_not_overdue_yet(mod, tmp_path):
    now = datetime.now(timezone.utc)
    write_pending(tmp_path, "soon", now - timedelta(minutes=2), expires_at=now + timedelta(hours=1))
    df = queue_df(materialize(component(mod, [tmp_path])))
    assert not bool(df.iloc[0]["overdue"]) and df.iloc[0]["expires_at"]


def test_overdue_by_expiry_even_when_young_and_error_severity(mod, tmp_path):
    now = datetime.now(timezone.utc)
    write_pending(tmp_path, "expired", now - timedelta(minutes=2), expires_at=now - timedelta(minutes=1))
    r = materialize(component(mod, [tmp_path], overdue_after_minutes=1000, overdue_severity="ERROR"))
    (ev,) = r.get_asset_check_evaluations()
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR


def test_unreadable_record_is_surfaced_as_overdue_not_hidden(mod, tmp_path):
    (tmp_path / "pending").mkdir()
    (tmp_path / "pending" / "broken.json").write_text("{not json")
    df = queue_df(materialize(component(mod, [tmp_path])))
    assert df.iloc[0]["key"] == "broken" and bool(df.iloc[0]["overdue"]) and "unreadable" in df.iloc[0]["summary"]


def test_remote_approval_dirs_via_fsspec(mod, agentic):
    import fsspec
    ask(agentic, "memory://queue-bucket/approvals", "R1", "Remote review")
    assert fsspec.filesystem("memory").exists("/queue-bucket/approvals/pending/R1.json")
    df = queue_df(materialize(component(mod, ["memory://queue-bucket/approvals"])))
    assert list(df["key"]) == ["R1"] and df.iloc[0]["approve_path"] == "memory://queue-bucket/approvals/R1.json"


def test_schedule_is_emitted_only_when_a_cron_is_set(mod, tmp_path):
    assert not component(mod, [tmp_path]).build_defs(None).schedules
    (sched,) = component(mod, [tmp_path], cron_schedule="*/5 * * * *").build_defs(None).schedules
    assert sched.name == "agent_review_queue_refresh" and sched.cron_schedule == "*/5 * * * *"


def test_config_validation(mod, tmp_path):
    with pytest.raises(ValueError, match="overdue_severity"):
        component(mod, [tmp_path], overdue_severity="LOUD").build_defs(None)
    with pytest.raises(ValueError, match="at least one"):
        mod.AgentReviewQueueComponent(approval_dirs=[]).build_defs(None)


def test_loose_field_shapes_are_accepted(mod, tmp_path):
    c = mod.AgentReviewQueueComponent(approval_dirs=[str(tmp_path)], kinds="agent, review", owners="team:ai", asset_tags={"n": 1})
    assert c.kinds == ["agent", "review"] and c.owners == ["team:ai"] and c.asset_tags == {"n": "1"}


def test_parked_requests_show_checkpoint_age_and_resume_count(mod, agentic, tmp_path):
    """A request whose run ENDED at an `approve: {mode: park}` gate: parked, how old its checkpoint is, how many resumes it has used."""
    ctx = SimpleNamespace(log=logging.getLogger("t"), run_id="run-1", instance=None)
    state = {"source": {"text": "payload"}, "draft": {"text": "Refund $120?"}, "__ctx__": {"partition_key": None, "partition_fields": None},
             "__pipeline__": {"prefix": "q", "asset": "q_draft"}}
    gate = {"id": "gate", "op": "approve", "source": "draft", "approval_dir": str(tmp_path), "key": "REF-1", "mode": "park", "park_expires_hours": 4}
    agentic._run_step(gate, state, ctx)
    agentic._park_write_checkpoint(state, ctx, asset_ids=["draft"], selected_outputs=None, pipeline_version="v", pv={})
    ask(agentic, tmp_path, "OTHER", "A plain wait-mode request")           # not parked

    df = queue_df(materialize(component(mod, [tmp_path]))).set_index("key")
    row = df.loc["REF-1"]
    assert bool(row["parked"]) and row["resume_count"] == 0 and 0 <= row["checkpoint_age_minutes"] < 5
    assert row["checkpoint_id"].startswith("q.gate.REF-1.") and row["expires_at"]
    assert not bool(df.loc["OTHER"]["parked"]) and pd.isna(df.loc["OTHER"]["resume_count"])

    agentic._load_resumed_state(ctx, dirs=[str(tmp_path / "checkpoints")], ckpt_id=row["checkpoint_id"], base_state={}, partition_key=None,
                                pipeline_version="v")
    df = queue_df(materialize(component(mod, [tmp_path]))).set_index("key")
    assert df.loc["REF-1"]["resume_count"] == 1
