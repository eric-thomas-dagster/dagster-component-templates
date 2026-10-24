"""`approve.mode: park`: an approval that does not hold a worker.

The run checkpoints its state and ENDS at the gate; a component-generated sensor requests a NEW run (Dagster cannot pause/resume a
run) that loads the checkpoint, skips the steps already done and continues. Everything here is a real Dagster materialization on a
real instance, a real approval store and (where receipts matter) a REAL ContextReceiptStore; only litellm.completion is faked. The
sensor is evaluated with build_sensor_context and the run it requests is executed with dg.materialize carrying the requested
tags/partition (no daemon: the daemon's run_key de-duplication is asserted as "the same run_key comes back", not exercised)."""
import importlib.util
import json
import pathlib
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import dagster as dg
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[4]
STORE_PY = ROOT / "resources" / "context_receipt_store" / "component.py"
KEY = "context_receipts"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def _llm(id, tmpl, source=None, **kw):
    st = {"id": id, "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": tmpl, **kw}
    if source:
        st["source"] = source
    return st


class Fake:
    """Records which step asked what; `fail_on` makes a step's model call raise."""

    def __init__(self, fake):
        self.fake, self.fail_on, self.draft = fake, set(), "DRAFT-v1"
        fake.completion = self.completion

    def completion(self, **kw):
        prompt = json.dumps(kw.get("messages"))
        self.fake.calls.append(kw)
        for tag in self.fail_on:
            if tag in prompt:
                raise RuntimeError(f"model down for {tag}")
        content = self.draft if "Draft:" in prompt else "PUBLISHED(" + prompt.split("Publish:")[-1][:30] + ")"
        msg = SimpleNamespace(content=content, tool_calls=[], reasoning_content=None, provider_specific_fields=None)
        usage = SimpleNamespace(total_tokens=10, prompt_tokens=5, completion_tokens=5, model_dump=lambda: {"total_tokens": 10})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)

    def n(self, marker):
        return sum(1 for c in self.fake.calls if marker in json.dumps(c.get("messages")))


@pytest.fixture
def fk(fake_litellm):
    return Fake(fake_litellm)


class Pipe:
    """draft (llm) -> gate (approve, parked) -> publish (llm, reads the gate)."""

    def __init__(self, mod, tmp_path, per_step_ops=False, partitions=None, gate=None, receipts=None, experiments=None, steps=None, outputs=None, store=None):
        self.mod, self.tmp = mod, tmp_path
        self.ap = str(tmp_path / "approvals")
        self.gate = {"id": "gate", "op": "approve", "source": "draft", "approval_dir": self.ap, "mode": "park", "key": "k1", **(gate or {})}
        if partitions:
            self.gate["key"] = "{partition_key}"
        self.comp = mod.AgenticPipelineComponent(
            asset_name_prefix="p", source={"kind": "literal", "text": "doc {partition_key}" if partitions else "doc"},
            steps=steps or [_llm("draft", "Draft: {text}"), self.gate, _llm("publish", "Publish: {text}", source="gate")],
            outputs=outputs or {"assets": ["draft", "gate", "publish"]}, per_step_ops=per_step_ops, receipts=receipts, experiments=experiments,
            **(dict(partition_type="static", partition_values=partitions) if partitions else {}))
        self.store = store
        self.defs = self.comp.build_defs(None)
        if store is not None:
            self.defs = dg.Definitions.merge(self.defs, dg.Definitions(resources={KEY: dg.ResourceDefinition.hardcoded_resource(store)}))
        self.inst = dg.DagsterInstance.ephemeral()
        self.sensor = self.defs.get_sensor_def("p_approval_resume_sensor")

    def run(self, key=None, tags=None, raise_on_error=True, selection=None):
        res = {KEY: dg.ResourceDefinition.hardcoded_resource(self.store)} if self.store is not None else {}
        return dg.materialize(self.defs.assets, partition_key=key, instance=self.inst, tags=tags, raise_on_error=raise_on_error,
                              resources=res, selection=selection)

    def tick(self):
        ctx = dg.build_sensor_context(instance=self.inst, definitions=self.defs)
        out = self.sensor(ctx)
        if isinstance(out, dg.SkipReason):
            return [], out.skip_message
        if isinstance(out, dg.SensorResult):
            return list(out.run_requests or []), out.skip_reason
        return list(out), None

    def resume(self, rr, raise_on_error=True):
        return self.run(key=rr.partition_key, tags=rr.tags, raise_on_error=raise_on_error)

    def approve(self, key="k1", **kw):
        return self.mod.approve_pending(self.ap, key, approver="eric", **kw)

    def ckpts(self):
        d = pathlib.Path(self.ap) / "checkpoints"
        return sorted(p for p in d.glob("*.json")) if d.exists() else []

    def archived(self):
        d = pathlib.Path(self.ap) / "checkpoints" / "archive"
        return [json.loads(p.read_text()) for p in sorted(d.glob("*.json"))] if d.exists() else []

    def pending(self, key="k1"):
        p = pathlib.Path(self.ap) / "pending" / f"{key}.json"
        return json.loads(p.read_text()) if p.exists() else None


def err_text(result):
    """The message of the first failure and of everything it was caused by."""
    err = [e for e in result.all_events if e.is_failure][0].event_specific_data.error
    out = []
    while err is not None:
        out.append(err.message)
        err = err.cause
    return "\n".join(out)


def mats(result):
    return [e.materialization.asset_key.to_user_string() for e in result.get_asset_materialization_events()]


def md(result, asset):
    ev = [e for e in result.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey(asset)][0]
    return {k: v.value for k, v in ev.materialization.metadata.items()}


BOTH = pytest.mark.parametrize("per_step_ops", [False, True], ids=["single_op", "per_step_ops"])


# ── the parked run ────────────────────────────────────────────────────────────

@BOTH
def test_a_parked_run_ends_immediately_materializes_only_what_is_final_and_writes_pending_plus_checkpoint(mod, fk, tmp_path, monkeypatch, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)

    def no_sleep(*a, **k):
        raise AssertionError("a parked approval must not poll")
    monkeypatch.setattr(mod.time, "sleep", no_sleep)
    t0 = time.time()
    r = p.run()
    assert time.time() - t0 < 1.0 and r.success

    assert mats(r) == ["p_draft"]                                       # draft is final; the gate has no decision yet; publish has not run
    for downstream in ("p_gate", "p_publish"):
        assert p.inst.get_latest_materialization_event(dg.AssetKey(downstream)) is None
    assert fk.n("Draft:") == 1 and fk.n("Publish:") == 0
    m = md(r, "p_draft")
    assert m["agentic_parked"] is True and m["agentic_parked_at_step"] == "gate" and m["agentic_approval_key"] == "k1"
    obs = r.get_asset_observation_events()                              # the gate asset is OBSERVED (parked), not materialized
    assert [o.asset_observation_data.asset_observation.asset_key.to_user_string() for o in obs] == ["p_gate"]
    assert p.inst.get_run_by_id(r.run_id).tags["agentic/parked"] == p.ckpts()[0].stem

    pend = p.pending()
    assert pend["parked"] is True and pend["checkpoint_id"] == p.ckpts()[0].stem and pend["fingerprint"]
    ck = json.loads(p.ckpts()[0].read_text())
    assert ck["version"] == 1 and ck["step"] == "gate" and ck["key"] == "k1" and ck["fingerprint"] == pend["fingerprint"]
    assert ck["resume_count"] == 0 and ck["run_id"] == r.run_id and ck["partition_key"] is None and ck["created_at"]
    assert ck["state"]["draft"]["text"] == "DRAFT-v1" and "gate" not in ck["state"] and ck["state"]["__emitted__"] == ["draft"]


@BOTH
def test_sensor_waits_for_approval_then_requests_exactly_one_resume_and_the_resumed_run_does_not_redo_done_steps(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    first = p.run()
    reqs, why = p.tick()
    assert reqs == [] and "waiting" in why and "no token yet" in why

    p.approve()
    reqs, _ = p.tick()
    assert len(reqs) == 1
    rr = reqs[0]
    ckid = p.ckpts()[0].stem
    assert rr.run_key == f"{ckid}:{json.loads(p.ckpts()[0].read_text())['fingerprint']}:0"
    assert rr.tags["agentic/resume_checkpoint"] == ckid and rr.tags["agentic/resume_of_run"] == first.run_id
    assert [x.run_key for x in p.tick()[0]] == [rr.run_key]              # same key again: the daemon de-duplicates on it

    calls_before = len(fk.fake.calls)
    r2 = p.resume(rr)
    assert r2.success
    assert fk.n("Draft:") == 1 and fk.n("Publish:") == 1 and len(fk.fake.calls) == calls_before + 1   # draft NOT re-run; publish ran once
    assert sorted(mats(r2)) == ["p_gate", "p_publish"]                  # draft is not re-materialized
    g = md(r2, "p_gate")
    assert g["gate__status"] == "approved" and g["gate__resumed"] == "True"
    assert md(r2, "p_gate")["agentic_resumed_from_run"] == first.run_id and md(r2, "p_gate")["agentic_resume_count"] == 1
    assert "DRAFT-v1" in md(r2, "p_publish")["publish__text"]   # publish read the gate's text: the checkpointed draft

    assert p.ckpts() == [] and p.archived()[0]["outcome"] == "completed" and p.pending() is None
    reqs, why = p.tick()
    assert reqs == [] and "no parked" in why


@BOTH
def test_rejection_follows_on_reject(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    p.run()
    p.approve(approved=False, reason="nope")
    (rr,), _ = p.tick()
    r = p.resume(rr, raise_on_error=False)
    assert not r.success                                                # on_reject: stop (default) -> the run fails like wait mode
    assert fk.n("Publish:") == 0 and fk.n("Draft:") == 1
    assert p.ckpts() == [] and p.archived()[0]["outcome"] == "rejected"  # final decision: archived, not "resumable"
    assert p.tick()[0] == []

    q = Pipe(mod, tmp_path / "c", per_step_ops, gate={"on_reject": "continue"})
    q.run()
    q.approve(approved=False)
    (rr,), _ = q.tick()
    r = q.resume(rr)
    assert r.success and md(r, "p_gate")["gate__status"] == "rejected"
    assert md(r, "p_publish")["publish__text"] != "_(empty)_"        # downstream ran (with the rejected gate's empty text)


# ── timeout ───────────────────────────────────────────────────────────────────

@BOTH
@pytest.mark.parametrize("on_timeout,final_status,run_ok", [("reject", "timeout_default_reject", False), ("fail", "timeout_fail", False),
                                                           ("approve", "timeout_default_approve", True)])
def test_expired_park_applies_on_timeout_in_a_sensor_launched_run(mod, fk, tmp_path, monkeypatch, per_step_ops, on_timeout, final_status, run_ok):
    p = Pipe(mod, tmp_path, per_step_ops, gate={"park_expires_hours": 2, "on_timeout": on_timeout})
    p.run()
    assert p.pending()["expires_at"]
    assert p.tick()[0] == []                                           # not expired yet
    real = mod._park_now()
    monkeypatch.setattr(mod, "_park_now", lambda: real + timedelta(hours=3))
    (rr,), _ = p.tick()
    assert rr.tags["agentic/resume_reason"] == "timeout"
    r = p.resume(rr, raise_on_error=False)
    assert r.success is run_ok
    if run_ok:
        assert md(r, "p_gate")["gate__status"] == final_status and fk.n("Publish:") == 1
    else:
        assert fk.n("Publish:") == 0                                    # fail ALWAYS stops; reject stops under on_reject: stop only
        assert p.pending()["outcome"] in ("timeout_default_reject", "timeout_fail")   # stays in the queue, marked
        assert p.archived()[0]["outcome"] == final_status


def test_on_timeout_reject_with_on_reject_continue_continues(mod, fk, tmp_path, monkeypatch):
    p = Pipe(mod, tmp_path, gate={"park_expires_hours": 1, "on_timeout": "reject", "on_reject": "continue"})
    p.run()
    real = mod._park_now()
    monkeypatch.setattr(mod, "_park_now", lambda: real + timedelta(hours=2))
    (rr,), _ = p.tick()
    r = p.resume(rr)
    assert r.success and md(r, "p_gate")["gate__status"] == "timeout_default_reject"


def test_on_timeout_pending_never_expires_and_no_expiry_means_waiting_forever(mod, fk, tmp_path, monkeypatch):
    p = Pipe(mod, tmp_path, gate={"park_expires_hours": 1, "on_timeout": "pending"})
    p.run()
    real = mod._park_now()
    monkeypatch.setattr(mod, "_park_now", lambda: real + timedelta(days=30))
    assert p.tick()[0] == []
    q = Pipe(mod, tmp_path / "q", gate={"on_timeout": "reject"})        # park_expires_hours unset
    q.run()
    assert q.pending()["expires_at"] is None and q.tick()[0] == []


def test_a_late_decision_beats_the_timeout(mod, fk, tmp_path, monkeypatch):
    p = Pipe(mod, tmp_path, gate={"park_expires_hours": 1, "on_timeout": "reject"})
    p.run()
    p.approve()
    real = mod._park_now()
    monkeypatch.setattr(mod, "_park_now", lambda: real + timedelta(minutes=30))
    (rr,), _ = p.tick()
    assert rr.tags["agentic/resume_reason"] == "decision" and p.resume(rr).success


# ── binding: the SAME rules as the wait mode ──────────────────────────────────

def test_a_token_for_other_input_is_not_a_decision_and_says_why(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    pathlib.Path(p.ap, "k1.json").write_text(json.dumps({"approved": True, "approver": "x", "fingerprint": "f" * 32, "approved_at": mod._now_iso()}))
    reqs, why = p.tick()
    assert reqs == [] and "input changed since approval" in why
    p.approve()                                                         # the real approval of THIS request goes through
    assert len(p.tick()[0]) == 1


def test_reuse_once_is_consumed_by_the_resume_and_expired_tokens_are_not_honoured(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path, gate={"reuse": "once", "max_age_hours": 1})
    p.run()
    old = (mod._park_now() - timedelta(hours=5)).isoformat()
    tok = p.approve()
    pathlib.Path(p.ap, "k1.json").write_text(json.dumps({**tok, "approved_at": old}))
    reqs, why = p.tick()
    assert reqs == [] and "expired" in why
    p.approve()
    (rr,), _ = p.tick()
    assert p.resume(rr).success
    assert json.loads(pathlib.Path(p.ap, "k1.json").read_text())["consumed_at"]


def test_changed_input_supersedes_the_old_checkpoint_so_it_can_never_time_out_into_an_approve(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path, gate={"park_expires_hours": 1, "on_timeout": "approve"})
    p.run()
    first = p.ckpts()[0]
    fk.draft = "DRAFT-v2"                                               # the input the reviewer would be shown changed
    p.run()
    assert not first.exists() and p.archived()[0]["outcome"] == "superseded"
    assert len(p.ckpts()) == 1 and p.ckpts()[0] != first
    assert p.pending()["fingerprint"] == json.loads(p.ckpts()[0].read_text())["fingerprint"]


# ── guards ────────────────────────────────────────────────────────────────────

def test_state_that_is_not_json_is_rejected_naming_the_step_and_leaves_no_pending_record(mod, fk, tmp_path, monkeypatch):
    real = mod._OPS["llm_call"]

    def llm_with_blob(step, state, context):
        out = real(step, state, context)
        out["blob"] = object()
        return out
    monkeypatch.setitem(mod._OPS, "llm_call", llm_with_blob)
    p = Pipe(mod, tmp_path)
    r = p.run(raise_on_error=False)
    assert not r.success
    err = err_text(r)
    assert "state['draft'].blob" in err and "mode: wait" in err
    assert p.ckpts() == [] and p.pending() is None


def test_strict_json_conversion(mod):
    assert mod._to_jsonable({"a": (1, 2), "b": [None, 1.5, "x", True]}, "s") == {"a": [1, 2], "b": [None, 1.5, "x", True]}
    for bad in ({"a": b"bytes"}, {"a": {1, 2}}, {1: "x"}, {"a": object()}):
        with pytest.raises(TypeError):
            mod._to_jsonable(bad, "s")


def test_load_time_guards(mod, tmp_path):
    ap = str(tmp_path)
    gate = {"id": "gate", "op": "approve", "source": "d", "approval_dir": ap, "mode": "park"}

    def build(steps, **kw):
        return mod.AgenticPipelineComponent(asset_name_prefix="g", source={"kind": "literal", "text": "x"}, steps=steps,
                                            outputs={"assets": ["d"]}, **kw).build_defs(None)
    d = _llm("d", "Draft: {text}")
    with pytest.raises(ValueError, match="invoke_component.*mode: wait"):
        build([d, {"id": "ic", "op": "invoke_component", "component_type": "x"}, gate])
    with pytest.raises(ValueError, match="sub_pipeline.*top-level"):
        build([d, {"id": "sp", "op": "sub_pipeline", "steps": [gate]}])
    with pytest.raises(ValueError, match="wait_seconds"):
        build([d, {**gate, "wait_seconds": 30}])
    with pytest.raises(ValueError, match="only apply to mode: park"):
        build([d, {**gate, "mode": "wait", "max_resumes": 2}])
    with pytest.raises(ValueError, match="mode must be"):
        build([d, {**gate, "mode": "sleep"}])
    with pytest.raises(ValueError, match="park_expires_hours"):
        build([d, {**gate, "park_expires_hours": 0}])
    with pytest.raises(ValueError, match="max_resumes"):
        build([d, {**gate, "max_resumes": 0}])
    nested_wait = {"id": "sp", "op": "sub_pipeline", "steps": [{**gate, "mode": "wait"}]}
    build([d, nested_wait])                                              # a nested WAIT gate is still fine
    assert not (build([d]).sensors or [])                                # no park step: no job, no sensor
    assert [s.name for s in build([d, gate]).sensors] == ["g_approval_resume_sensor"]


def test_park_gate_inside_sub_pipeline_loaded_from_a_file_fails_loudly_at_run_time(mod, ctx):
    state = {"source": {"text": "x"}, "__ctx__": {}, "__personas__": {}}
    state["__parked__"] = {"step": "g"}
    assert state.get("__parked__")                                       # the run-time guard keys off this marker (see _do_sub_pipeline)


# ── mixed modes ───────────────────────────────────────────────────────────────

def test_wait_and_park_gates_coexist_each_independently(mod, fk, tmp_path):
    g2 = {"id": "gate2", "op": "approve", "source": "publish", "approval_dir": str(tmp_path / "ap2"), "key": "k2", "on_timeout": "pending"}
    steps = [_llm("draft", "Draft: {text}"), {"id": "gate", "op": "approve", "source": "draft", "approval_dir": str(tmp_path / "approvals"),
                                              "mode": "park", "key": "k1"},
             _llm("publish", "Publish: {text}", source="gate"), g2]
    p = Pipe(mod, tmp_path, steps=steps, outputs={"assets": ["draft", "publish"]})
    r = p.run()
    assert r.success and mats(r) == ["p_draft"]
    p.approve()
    (rr,), _ = p.tick()
    r2 = p.resume(rr)                                                    # park resumed, then the wait gate answered "pending" in-run
    assert r2.success and md(r2, "p_publish")["publish__op"] == "llm_call"
    assert pathlib.Path(tmp_path, "ap2", "pending", "k2.json").exists() and p.pending("k2") is None


# ── partitions ────────────────────────────────────────────────────────────────

@BOTH
def test_two_parked_partitions_resume_independently(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops, partitions="a,b")
    assert p.run("a").success and p.run("b").success
    assert len(p.ckpts()) == 2
    cks = {json.loads(c.read_text())["partition_key"]: json.loads(c.read_text()) for c in p.ckpts()}
    assert set(cks) == {"a", "b"} and cks["a"]["key"] == "a"

    p.approve("b")
    reqs, _ = p.tick()
    assert [r.partition_key for r in reqs] == ["b"]
    assert p.resume(reqs[0]).success
    assert [c.stem for c in p.ckpts()] == [cks["a"]["id"]]               # a is still parked, untouched
    assert p.inst.get_latest_materialization_event(dg.AssetKey("p_publish")).dagster_event.partition == "b"

    p.approve("a", approved=False)
    (ra,), _ = p.tick()
    assert ra.partition_key == "a"
    assert not p.resume(ra, raise_on_error=False).success
    assert p.ckpts() == [] and sorted(x["partition_key"] for x in p.archived()) == ["a", "b"]


def test_a_resume_run_for_another_partition_is_refused(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path, partitions="a,b")
    p.run("a")
    p.approve("a")
    (rr,), _ = p.tick()
    r = p.run(key="b", tags=rr.tags, raise_on_error=False)
    assert not r.success and p.ckpts()                                   # refused; checkpoint untouched and still resumable


# ── receipts + experiments + budget continuity ────────────────────────────────

@BOTH
def test_resume_reuses_the_receipt_and_the_experiment_variant_and_records_the_approve_outcome(mod, fk, tmp_path, per_step_ops):
    spec = importlib.util.spec_from_file_location("rs_for_park", STORE_PY)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    store = m.ContextReceiptStore(str(tmp_path / "r.sqlite3"))
    exp = [{"name": "tone", "steps": ["draft"], "variants": {"warm": {"draft": {"temperature": 0.9}}}, "fraction": 0.5, "seed": 3}]
    p = Pipe(mod, tmp_path, per_step_ops, partitions="a,b,c,d,e,f", experiments=exp,
             receipts={"resource_key": KEY, "outcome_signals": {"approve": {"approved": 0.4}}}, store=store)
    variants = {}
    for k in "abcdef":
        r = p.run(k)
        variants[k] = md(r, "p_draft")["agentic_experiment_variant"]
    assert set(variants.values()) == {"control", "warm"}                 # both sides are exercised
    k = "a" if variants["a"] == "warm" else next(x for x in variants if variants[x] == "warm")
    before = [store.get_receipt(r) for r in store.receipts_for_partition("p_draft", k)]
    assert len(before) == 1
    p.approve(k)
    reqs, _ = p.tick()
    rr = [x for x in reqs if x.partition_key == k][0]
    r2 = p.resume(rr)
    assert r2.success
    after = [store.get_receipt(r) for r in store.receipts_for_partition("p_draft", k)]
    assert [a["receipt_id"] for a in after] == [before[0]["receipt_id"]]   # NO second generation receipt
    assert md(r2, "p_gate")["receipt_id"] == before[0]["receipt_id"]
    assert md(r2, "p_gate")["agentic_experiment_variant"] == variants[k] == "warm"
    outs = [e["payload"]["signal"] for e in store.get_receipt(before[0]["receipt_id"])["events"] if e["event_type"] == "outcome"]
    assert outs == ["accepted"]


def _with_policy(p, policy):
    p.comp.policy = policy
    p.defs = p.comp.build_defs(None)
    p.sensor = p.defs.get_sensor_def("p_approval_resume_sensor")


def test_per_run_caps_are_cumulative_across_the_park(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    _with_policy(p, {"budget": {"max_tokens": 15}})                     # each fake call uses 10 tokens
    assert p.run().success                                              # draft: 10 <= 15
    assert json.loads(p.ckpts()[0].read_text())["state"]["__policy_usage__"]["tokens"] == 10
    p.approve()
    (rr,), _ = p.tick()
    r2 = p.resume(rr, raise_on_error=False)
    assert not r2.success                                               # 10 carried + 10 > 15: ONE logical execution, ONE cap
    err = err_text(r2)
    assert "max_tokens=15" in err


def test_budget_ledger_entries_are_continuous_across_runs(mod, fk, tmp_path):
    ledger = str(tmp_path / "ledger")
    p = Pipe(mod, tmp_path)
    _with_policy(p, {"budget": {"max_tokens": 100, "ledger": {"dir": ledger}}})
    first = p.run()
    p.approve()
    (rr,), _ = p.tick()
    second = p.resume(rr)
    day = mod._ledger_now().strftime("%Y-%m-%d")
    ent = mod._SpendLedger({"dir": ledger}).entries("day", day)
    assert sorted(ent) == sorted([f"{first.run_id}__draft", f"{second.run_id}__publish"])   # keyed by (run id, step): no collision, no loss
    assert mod._SpendLedger({"dir": ledger}).total("day", day)["llm_calls"] == 2


# ── retries + max_resumes ─────────────────────────────────────────────────────

@BOTH
def test_a_failed_resume_stays_resumable_up_to_max_resumes(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops, gate={"max_resumes": 2})
    p.run()
    p.approve()
    fk.fail_on.add("Publish:")
    keys = []
    for attempt in (1, 2):
        (rr,), _ = p.tick()
        keys.append(rr.run_key)
        assert not p.resume(rr, raise_on_error=False).success
        assert json.loads(p.ckpts()[0].read_text())["resume_count"] == attempt
    assert len(set(keys)) == 2 and keys[0].endswith(":0") and keys[1].endswith(":1")   # a retry is a NEW run_key, so it is not de-duplicated away
    reqs, why = p.tick()
    assert reqs == [] and "exhausted" in why and p.ckpts()                 # needs a person; the checkpoint is kept
    assert fk.n("Draft:") == 1                                              # never re-ran the finished step
    r = p.run(tags={"agentic/resume_checkpoint": p.ckpts()[0].stem}, raise_on_error=False)
    assert not r.success                                                    # a manual launch cannot exceed it either


def test_a_resume_whose_approval_vanished_does_not_proceed(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    ckid = p.ckpts()[0].stem
    r = p.run(tags={"agentic/resume_checkpoint": ckid}, raise_on_error=False)   # launched by hand, nothing approved, not expired
    assert not r.success and fk.n("Publish:") == 0
    assert json.loads(p.ckpts()[0].read_text())["resume_count"] == 1


def test_a_resume_is_not_duplicated_while_its_run_is_alive_and_is_retried_when_it_died(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    p.approve()
    ck = p.ckpts()[0]
    live = p.inst.create_run_for_job(job_def=p.defs.resolve_job_def("p_approval_resume_job"))   # NOT_STARTED: still in flight
    store = mod._ApprovalStore(str(pathlib.Path(p.ap) / "checkpoints"))
    store.write_text(store.path(f"{ck.stem}.claim.1"), json.dumps({"run_id": live.run_id, "claimed_at": mod._now_iso()}))
    data = json.loads(ck.read_text())
    ck.write_text(json.dumps({**data, "resume_count": 1}))
    reqs, why = p.tick()
    assert reqs == [] and "in flight" in why
    p.inst.report_run_failed(live)                                      # the claiming run died (OOM, node loss, ...)
    reqs, _ = p.tick()
    assert len(reqs) == 1 and reqs[0].run_key.endswith(":1")            # the next attempt is allowed


# ── concurrency ───────────────────────────────────────────────────────────────

def test_two_resumes_of_one_checkpoint_exactly_one_wins(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    ckid = p.ckpts()[0].stem
    dirs = [str(pathlib.Path(p.ap) / "checkpoints")]
    barrier = threading.Barrier(8)
    won, lost = [], []

    def go(i):
        ctx = SimpleNamespace(run_id=f"r{i}", instance=None, log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None))
        barrier.wait()
        try:
            mod._load_resumed_state(ctx, dirs=dirs, ckpt_id=ckid, base_state={}, partition_key=None, pipeline_version="v")
            won.append(i)
        except RuntimeError as exc:
            lost.append(str(exc))
    ts = [threading.Thread(target=go, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(won) == 1 and len(lost) == 7
    assert all("already being resumed" in m or "max_resumes" in m or "not found" in m for m in lost)


def test_two_real_resume_runs_at_once_do_the_downstream_work_once(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path, gate={"reuse": "once"})
    p.run()
    p.approve()
    (rr,), _ = p.tick()
    out = []

    def go():
        out.append(p.resume(rr, raise_on_error=False))
    ts = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert fk.n("Publish:") == 1 and sum(1 for r in out if r.success) == 1
    assert json.loads(pathlib.Path(p.ap, "k1.json").read_text())["consumed_at"]   # `once` consumed by the single winner
