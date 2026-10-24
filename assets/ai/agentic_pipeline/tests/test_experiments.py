"""A/B experiments judged by real outcomes: generation receipts, deterministic variant assignment, override scoping,
automatic outcome signals and the `experiment_verdict` check.

Real Dagster materializations, a REAL ContextReceiptStore (loaded by path); only litellm.completion (and its price) is faked."""
import ast
import hashlib
import importlib.util
import json
import pathlib
from types import SimpleNamespace

import dagster as dg
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[4]
STORE_PY = ROOT / "resources" / "context_receipt_store" / "component.py"
RAG_PY = ROOT / "assets" / "ai" / "rag_pipeline" / "component.py"
KEY = "context_receipts"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def store(tmp_path):
    return _load(STORE_PY, "receipt_store_for_agentic_tests").ContextReceiptStore(str(tmp_path / "r.sqlite3"))


def script(fake, fn):
    """Install `fn(kwargs) -> content` as the model; every call is also kept in fake.calls."""
    def completion(**kw):
        fake.calls.append(kw)
        msg = SimpleNamespace(content=fn(kw), tool_calls=[], reasoning_content=None, provider_specific_fields=None)
        usage = SimpleNamespace(total_tokens=10, prompt_tokens=5, completion_tokens=5, model_dump=lambda: {"total_tokens": 10})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)
    fake.completion = completion


DRAFT = {"id": "draft", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "Draft: {text}"}
POLISH = {"id": "polish", "op": "llm_call", "source": "draft", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY",
          "prompt_template": "Polish: {text}", "system_prompt": "Be terse"}
CAND = {"polish": {"model": "gpt-4o", "system": "Be warm", "temperature": 0.9}}


def exp(**kw):
    return [{"name": "tone", "steps": ["polish"], "variants": {"warm": CAND}, "fraction": 0.5, "seed": 3, **kw}]


def comp(mod, *, experiments=None, receipts=None, steps=None, per_step_ops=False, partitions="p0,p1,p2,p3,p4,p5", outputs=None, **kw):
    pkw = dict(partition_type="static", partition_values=partitions) if partitions else {}
    return mod.AgenticPipelineComponent(
        asset_name_prefix="e", source={"kind": "literal", "text": "doc {partition_key}" if partitions else "doc"},
        steps=steps or [dict(DRAFT), dict(POLISH)], outputs=outputs or {"assets": ["draft", "polish"]}, per_step_ops=per_step_ops,
        experiments=experiments, receipts=receipts, **pkw, **kw)


def run(c, store, key=None, instance=None, raise_on_error=True):
    return dg.materialize(c.build_defs(None).assets, partition_key=key, instance=instance, raise_on_error=raise_on_error,
                          resources={KEY: dg.ResourceDefinition.hardcoded_resource(store)})


def md(result, asset="e_polish"):
    ev = [e for e in result.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey(asset)][0]
    return {k: v.value for k, v in ev.materialization.metadata.items()}


def receipts_of(store, key, asset="e_draft"):
    return [store.get_receipt(r) for r in store.receipts_for_partition(asset, key)]


def side(mod, key, e=None):
    return mod._experiment_assigned_candidate({"name": "tone", "seed": 3, "fraction": 0.5, **(e or {})}, key)


# ── assignment ────────────────────────────────────────────────────────────────

def test_assignment_is_deterministic_and_the_fraction_is_roughly_honoured(mod):
    e = {"name": "tone", "seed": 7, "fraction": 0.3}
    keys = [f"2026-01-{i:03d}" for i in range(200)]
    first = [mod._experiment_assigned_candidate(e, k) for k in keys]
    assert first == [mod._experiment_assigned_candidate(e, k) for k in keys]                    # pure: same unit, same side
    assert 40 <= sum(first) <= 80, sum(first)                                                   # 30% of 200 (binomial sd ~ 6.5)
    assert first != [mod._experiment_assigned_candidate({**e, "seed": 8}, k) for k in keys]     # the seed matters
    assert first != [mod._experiment_assigned_candidate({**e, "name": "other"}, k) for k in keys]   # so does the experiment
    assert all(mod._experiment_assigned_candidate({**e, "fraction": 1}, k) for k in keys)       # fraction 1 = everyone


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_a_partition_gets_the_same_variant_on_every_rerun(mod, fake_litellm, store, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, experiments=exp(), receipts={"resource_key": KEY}, per_step_ops=per_step_ops)
    for _ in range(2):
        assert md(run(c, store, "p1"))["agentic_experiment_variant"] == ("warm" if side(mod, "p1") else "control")
    assert {r["purpose"] for r in receipts_of(store, "p1")} == {f"experiment:tone:{'warm' if side(mod, 'p1') else 'control'}"}


# ── overrides apply to the named steps only ───────────────────────────────────

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_overrides_apply_to_the_named_step_only(mod, fake_litellm, store, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, experiments=exp(), receipts={"resource_key": KEY}, per_step_ops=per_step_ops)
    cand_key = next(k for k in ("p0", "p1", "p2", "p3", "p4", "p5") if side(mod, k))
    ctrl_key = next(k for k in ("p0", "p1", "p2", "p3", "p4", "p5") if not side(mod, k))
    for key, cand in ((cand_key, True), (ctrl_key, False)):
        fake_litellm.calls.clear()
        run(c, store, key)
        draft, polish = fake_litellm.calls
        assert draft["model"] == "gpt-4o-mini" and draft.get("temperature") != 0.9   # untouched step
        assert "Draft:" in draft["messages"][-1]["content"]
        assert polish["model"] == ("gpt-4o" if cand else "gpt-4o-mini")
        assert polish["messages"][0]["content"] == ("Be warm" if cand else "Be terse")
        assert (polish.get("temperature") == 0.9) is cand
        assert polish["messages"][-1]["content"].startswith("Polish:")          # the prompt template was not overridden


def test_the_configured_step_is_never_mutated(mod, fake_litellm, store):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, experiments=exp(fraction=1), receipts={"resource_key": KEY})
    before = json.dumps(c.steps, sort_keys=True)
    run(c, store, "p0")
    assert json.dumps(c.steps, sort_keys=True) == before and fake_litellm.calls[-1]["model"] == "gpt-4o"


# ── versions: effective per variant, and the base asset code_version is untouched ──

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_the_effective_version_is_what_the_receipt_and_metadata_record(mod, fake_litellm, store, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, experiments=exp(), receipts={"resource_key": KEY}, per_step_ops=per_step_ops)
    base = mod.prompt_versions({"steps": c.steps, "personas": None})["polish"]["version"]
    cand_steps = [dict(s, **{"model": "gpt-4o", "system_prompt": "Be warm", "temperature": 0.9}) if s["id"] == "polish" else s for s in c.steps]
    cand = mod.prompt_versions({"steps": cand_steps, "personas": None})["polish"]["version"]
    assert base != cand
    for key in ("p0", "p1", "p2", "p3", "p4", "p5"):
        is_cand = side(mod, key)
        r = run(c, store, key)
        want = cand if is_cand else base
        m = md(r)
        assert m["agentic_prompt_version"] == want and m["agentic_receipt_code_version"] == want
        assert m["agentic_prompt_versions"]["polish"] == want and m["agentic_prompt_versions"]["draft"] == md(r, "e_draft")["agentic_prompt_version"]
        rec = receipts_of(store, key)[-1]
        assert [i["source_code_version"] for i in rec["items"]] == [want]


def test_an_experiment_does_not_change_the_assets_code_version(mod):
    plain = comp(mod).build_defs(None).assets[0]
    with_exp = comp(mod, experiments=exp(), receipts={"resource_key": KEY}).build_defs(None).assets[0]
    for k in ("e_draft", "e_polish"):
        assert with_exp.get_asset_spec(dg.AssetKey(k)).code_version == plain.get_asset_spec(dg.AssetKey(k)).code_version
    ps = comp(mod, experiments=exp(), per_step_ops=True, receipts={"resource_key": KEY}).build_defs(None).assets[0]
    for k in ("e_draft", "e_polish"):
        assert ps.get_asset_spec(dg.AssetKey(k)).code_version == comp(mod, per_step_ops=True).build_defs(None).assets[0].get_asset_spec(dg.AssetKey(k)).code_version


# ── receipts ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_one_generation_receipt_per_run_with_the_right_shape(mod, fake_litellm, store, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, receipts={"resource_key": KEY, "caller_id": "svc", "policy_version": "pol-1"}, per_step_ops=per_step_ops)
    r1 = run(c, store, "p2")
    r2 = run(c, store, "p3")
    (a,), (b,) = receipts_of(store, "p2"), receipts_of(store, "p3")                                  # exactly one each
    assert md(r1)["receipt_id"] == a["receipt_id"] and md(r2)["receipt_id"] == b["receipt_id"]       # emitted as asset metadata
    assert md(r1, "e_draft")["receipt_id"] == a["receipt_id"]
    assert a["purpose"] == "generation" and a["context_name"] == "e_draft" and a["dagster_asset_key"] == "e_draft"
    assert a["dagster_partition_key"] == "p2" and a["dagster_run_id"] == r1.run_id and a["caller_id"] == "svc" and a["policy_version"] == "pol-1"
    (item,) = a["items"]
    assert item["item_id"] == "e_draft:p2" and item["source_asset_key"] == "e_draft" and item["source_partition"] == "p2"
    assert item["source_code_version"].startswith("pp-") and a["items"][0]["source_code_version"] == b["items"][0]["source_code_version"]
    assert "agentic_experiment" not in md(r1)


def test_without_an_experiment_the_code_version_is_the_whole_pipeline_prompt_hash(mod, fake_litellm, store):
    script(fake_litellm, lambda kw: "ok")
    run(comp(mod, receipts={"resource_key": KEY}), store, "p0")
    changed = dict(POLISH, prompt_template="Polish harder: {text}")
    run(comp(mod, receipts={"resource_key": KEY}, steps=[dict(DRAFT), changed]), store, "p0")
    run(comp(mod, receipts={"resource_key": KEY}, steps=[dict(DRAFT), dict(POLISH, source="draft", id="polish", description="wiring only")]), store, "p0")
    cvs = [r["items"][0]["source_code_version"] for r in receipts_of(store, "p0")]
    assert cvs[0] != cvs[1] and cvs[0] == cvs[2]            # a prompt edit is a new recipe; a description edit is not


def test_unpartitioned_run_uses_the_run_id_as_the_unit(mod, fake_litellm, store):
    script(fake_litellm, lambda kw: "ok")
    r = run(comp(mod, partitions=None, receipts={"resource_key": KEY}), store)
    rec = store.get_receipt(md(r)["receipt_id"])
    assert rec["dagster_partition_key"] is None and rec["items"][0]["item_id"] == f"e_draft:{r.run_id}"
    assert rec["items"][0]["source_partition"] is None


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_receipt_id_reaches_the_sinks(mod, fake_litellm, store, tmp_path, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    outputs = {"assets": ["draft", "polish"],
               "text_sinks": [{"from": "polish", "path": str(tmp_path / "t" / "{partition_key}-{receipt_id}.txt")}],
               "json_sinks": [{"from": "polish", "path": str(tmp_path / "j" / "{receipt_id}.json")}]}
    r = run(comp(mod, receipts={"resource_key": KEY}, outputs=outputs, experiments=exp(fraction=1), per_step_ops=per_step_ops), store, "p4")
    rid = md(r)["receipt_id"]
    assert (tmp_path / "t" / f"p4-{rid}.txt").read_text() == "ok"
    body = json.loads((tmp_path / "j" / f"{rid}.json").read_text())
    assert body["receipt_id"] == rid and body["experiment"] == "tone" and body["experiment_variant"] == "warm" and body["text"] == "ok"


def test_receipt_id_in_a_sink_without_receipts_is_rejected_at_load(mod):
    outputs = {"assets": ["draft"], "text_sinks": [{"from": "draft", "path": "/tmp/x/{receipt_id}.txt"}]}
    with pytest.raises(ValueError, match="receipt_id"):
        comp(mod, outputs=outputs).build_defs(None)


class Boom:
    def record_retrieval(self, **kw):
        raise RuntimeError("store down")

    def record_outcome(self, *a, **k):
        raise RuntimeError("store down")


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_a_failing_store_never_fails_the_run_unless_fail_on_error(mod, fake_litellm, tmp_path, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    outputs = {"assets": ["draft", "polish"], "text_sinks": [{"from": "polish", "path": str(tmp_path / "{receipt_id}.txt")}]}
    soft = comp(mod, receipts={"resource_key": KEY}, outputs=outputs, per_step_ops=per_step_ops)
    r = run(soft, Boom(), "p0")
    assert r.success and md(r)["receipt_id"] == "unrecorded" and (tmp_path / "unrecorded.txt").exists()
    hard = comp(mod, receipts={"resource_key": KEY, "fail_on_error": True}, per_step_ops=per_step_ops)
    r = run(hard, Boom(), "p0", raise_on_error=False)
    assert not r.success
    assert "store down" in " ".join(str(e.event_specific_data) for e in r.all_events if e.is_failure or e.is_step_failure)


# ── automatic outcome signals ─────────────────────────────────────────────────

def outcomes(store, rec):
    return [(e["payload"]["signal"], e["payload"]["value"], e["payload"]["outcome_ref"], e["payload"]["source"])
            for e in store.get_receipt(rec["receipt_id"])["events"] if e["event_type"] == "outcome"]


def gated(tmp_path, **gate):
    return [dict(DRAFT), {"id": "gate", "op": "approve", "source": "draft", "approval_dir": str(tmp_path / "ap"),
                          "on_timeout": "reject", "wait_seconds": 0, "on_reject": "continue", **gate}]


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_approve_decisions_become_outcomes_through_a_real_approval_flow(mod, fake_litellm, store, tmp_path, per_step_ops):
    script(fake_litellm, lambda kw: "ok")
    outs = {"assets": ["draft", "gate"]}
    c = comp(mod, steps=gated(tmp_path), outputs=outs, per_step_ops=per_step_ops,
             receipts={"resource_key": KEY, "outcome_signals": {"approve": {"approved": 0.4, "rejected": -0.9}}})
    run(c, store, "p0")                                          # nobody decided yet: timeout default, nothing mapped
    (r0,) = receipts_of(store, "p0")
    assert outcomes(store, r0) == []
    mod.approve_pending(str(tmp_path / "ap"), "p0", approved=True)
    run(c, store, "p0")
    assert outcomes(store, receipts_of(store, "p0")[-1]) == [("accepted", 0.4, "auto:gate:approve", "agentic_pipeline:approve")]
    run(c, store, "p1")
    mod.approve_pending(str(tmp_path / "ap"), "p1", approved=False, reason="no")
    run(c, store, "p1")
    assert outcomes(store, receipts_of(store, "p1")[-1]) == [("escalated", -0.9, "auto:gate:approve", "agentic_pipeline:approve")]


def test_approve_defaults_timeout_mapping_and_the_stop_path(mod, fake_litellm, store, tmp_path):
    script(fake_litellm, lambda kw: "ok")
    outs = {"assets": ["draft", "gate"]}
    c = comp(mod, steps=gated(tmp_path), outputs=outs, receipts={"resource_key": KEY, "outcome_signals": True})
    run(c, store, "p0")                                                         # timeout: unmapped by default -> no outcome
    assert outcomes(store, receipts_of(store, "p0")[0]) == []
    mod.approve_pending(str(tmp_path / "ap"), "p0", approved=False)
    run(c, store, "p0")
    assert outcomes(store, receipts_of(store, "p0")[-1])[0][:2] == ("escalated", -0.7)      # the default mapping
    t = comp(mod, steps=gated(tmp_path / "t"), outputs=outs, receipts={"resource_key": KEY, "outcome_signals": {"approve": {"timeout": 0}}})
    run(t, store, "p2")
    assert outcomes(store, receipts_of(store, "p2")[-1])[0][:3] == ("approval_timeout", 0.0, "auto:gate:approve")


def test_the_rejection_that_stops_the_pipeline_is_still_recorded(mod, fake_litellm, store, tmp_path):
    script(fake_litellm, lambda kw: "ok")
    steps = gated(tmp_path, on_reject="stop")
    c = comp(mod, steps=steps, outputs={"assets": ["draft", "gate"]}, receipts={"resource_key": KEY, "outcome_signals": {"approve": {"timeout": -0.2}}})
    assert not run(c, store, "p5", raise_on_error=False).success                # timeout default reject -> stop
    assert outcomes(store, receipts_of(store, "p5")[-1])[0][:2] == ("approval_timeout", -0.2)


def test_verify_and_eval_results_become_outcomes(mod, fake_litellm, store):
    def model(kw):
        sysmsg = kw["messages"][0]["content"]
        if "verify" in sysmsg.lower():
            return json.dumps({"verdict": "supported" if "doc p0" in kw["messages"][-1]["content"] else "unsupported",
                               "score": 0.9 if "doc p0" in kw["messages"][-1]["content"] else 0.1})
        if "evaluator" in sysmsg.lower():
            return json.dumps({"scores": {"overall quality": {"score": 4, "rationale": "ok"}}})
        return "doc p0 answer" if "p0" in kw["messages"][-1]["content"] else "other"
    script(fake_litellm, model)
    steps = [dict(DRAFT), {"id": "ver", "op": "verify", "source": "draft", "evidence_source": "draft", "model": "gpt-4o-mini",
                           "api_key_env_var": "OPENAI_API_KEY"},
             {"id": "ev", "op": "eval", "source": "draft", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "scale": 5}]
    c = comp(mod, steps=steps, outputs={"assets": ["draft", "ver", "ev"]}, receipts={"resource_key": KEY, "outcome_signals": True})
    run(c, store, "p0")
    run(c, store, "p1")
    o0, o1 = (outcomes(store, receipts_of(store, k)[-1]) for k in ("p0", "p1"))
    by = lambda o: {ref: (sig, val) for sig, val, ref, _ in o}  # noqa: E731
    assert by(o0)["auto:ver:verify"] == ("verify_passed", 0.5) and by(o1)["auto:ver:verify"] == ("verify_failed", -0.7)
    assert by(o0)["auto:ev:eval"] == ("eval_score", pytest.approx(0.5))          # 4 on a 1..5 scale -> 2*(4-1)/(5-1)-1
    custom = comp(mod, steps=steps, outputs={"assets": ["draft", "ver", "ev"]},
                  receipts={"resource_key": KEY, "outcome_signals": {"eval": {"min": 0, "max": 8}, "verify": False}})
    run(custom, store, "p2")
    (only,) = outcomes(store, receipts_of(store, "p2")[-1])
    assert only[:3] == ("eval_score", pytest.approx(0.0), "auto:ev:eval")        # 4 on 0..8 -> 0


def test_a_repeated_outcome_ref_supersedes_instead_of_double_counting(mod, fake_litellm, store, tmp_path):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, steps=gated(tmp_path), outputs={"assets": ["draft", "gate"]}, receipts={"resource_key": KEY, "outcome_signals": True})
    run(c, store, "p0")
    mod.approve_pending(str(tmp_path / "ap"), "p0", approved=True)
    r = run(c, store, "p0")
    rec = receipts_of(store, "p0")[-1]
    state = {"__receipts__": {"resource_key": KEY, "receipt_id": rec["receipt_id"], "fail_on_error": True,
                              "signals": mod._norm_signals(True)}}
    ctx = SimpleNamespace(resources=SimpleNamespace(**{KEY: store}), log=None)
    for _ in range(3):                                              # an op retry re-emitting the same event
        mod._auto_signal(state, ctx, {"id": "gate"}, "approve", status="approved")
    assert len(outcomes(store, rec)) == 4 and r.success
    assert store.outcome_samples_by_code_version("e_draft")[rec["items"][0]["source_code_version"]].count(0.5) >= 1
    vals = [v for cv in store.outcome_samples_by_code_version("e_draft").values() for v in cv]
    assert vals == [0.5]                                            # one effective outcome for the receipt, not four


# ── end to end: the verdict ───────────────────────────────────────────────────

RATE = {"gpt-4o-mini": 0.3, "gpt-4o": 0.92}


def reviewer_model(kw):
    """A deterministic 'model' whose better variant produces text a reviewer approves more often."""
    user = kw["messages"][-1]["content"]
    u = int(hashlib.sha256(f"{kw['model']}|{user}".encode()).hexdigest()[:8], 16) / 16 ** 8
    return "GOOD answer" if u < RATE[kw["model"]] else "weak answer"


def verdict_check(mod, c, store, instance, window=None):
    defs = c.build_defs(None)
    chk = defs.asset_checks[0]
    job = dg.define_asset_job("verdict", selection=dg.AssetSelection.checks(*chk.check_keys))
    d = dg.Definitions(assets=defs.assets, asset_checks=defs.asset_checks, jobs=[job],
                       resources={KEY: dg.ResourceDefinition.hardcoded_resource(store)})
    res = d.get_job_def("verdict").execute_in_process(instance=instance)
    assert res.success
    (ev,) = res.get_asset_check_evaluations()
    return ev


def e2e(mod, fake_litellm, store, tmp_path, n, per_step_ops=False, extra=None):
    script(fake_litellm, reviewer_model)
    keys = [f"d{i:02d}" for i in range(n)]
    steps = [dict(DRAFT, id="gen"), {"id": "gate", "op": "approve", "source": "gen", "approval_dir": str(tmp_path / "ap"),
                                     "on_timeout": "reject", "wait_seconds": 0, "on_reject": "continue"}]
    c = comp(mod, steps=steps, outputs={"assets": ["gen", "gate"]}, partitions=",".join(keys), per_step_ops=per_step_ops,
             experiments=[{"name": "model-up", "steps": ["gen"], "variants": {"big": {"gen": {"model": "gpt-4o"}}},
                           "fraction": 0.5, "seed": 11, "min_outcomes": 10, **(extra or {})}],
             receipts={"resource_key": KEY, "outcome_signals": {"approve": True}})
    inst = dg.DagsterInstance.ephemeral()
    for k in keys:
        run(c, store, k, instance=inst)                       # run 1: every partition lands in the review queue
    for k in keys:
        pend = json.loads((tmp_path / "ap" / "pending" / f"{k}.json").read_text())
        mod.approve_pending(str(tmp_path / "ap"), k, approved="GOOD" in pend["preview"], approver="reviewer")
    for k in keys:
        run(c, store, k, instance=inst)                       # run 2: the decisions are honoured
    return c, inst


def test_end_to_end_the_better_variant_wins_the_verdict(mod, fake_litellm, store, tmp_path):
    c, inst = e2e(mod, fake_litellm, store, tmp_path, 60)
    ev = verdict_check(mod, c, store, inst)
    m = {k: v.value for k, v in ev.metadata.items()}
    assert m["verdict"] == "candidate_better", m
    assert ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert m["n_control"] >= 10 and m["n_candidate"] >= 10 and m["n_control"] + m["n_candidate"] == 60   # one outcome per decided receipt
    assert m["diff"] > 0.5 and m["ci_low"] > 0 and m["p_candidate_better"] > 0.99 and m["ready_to_promote"] == "n/a"
    assert m["control_code_version"] != m["candidate_code_version"] and m["candidate_label"] == "big"
    assert m["control_cost_usd_mean"] == pytest.approx(0.01) and m["candidate_cost_usd_mean"] == pytest.approx(0.01)   # cost per variant
    by = store.outcomes_by_code_version("e_gen")
    assert set(by) == {m["control_code_version"], m["candidate_code_version"]}
    assert by[m["candidate_code_version"]]["mean_outcome"] > by[m["control_code_version"]]["mean_outcome"]
    assert sum(v["receipts"] for v in by.values()) == 120 and sum(v["outcomes"] for v in by.values()) == 60


def test_ready_to_promote_is_reported_and_nothing_is_promoted(mod, fake_litellm, store, tmp_path):
    c, inst = e2e(mod, fake_litellm, store, tmp_path, 60, extra={"promote_when": {"confidence": 0.9, "min_outcomes": 20}})
    m = {k: v.value for k, v in verdict_check(mod, c, store, inst).metadata.items()}
    assert m["ready_to_promote"] is True
    assert [s["model"] for s in c.steps if s["id"] == "gen"] == ["gpt-4o-mini"]     # the base config is untouched


def test_too_little_evidence_is_insufficient_not_a_verdict(mod, fake_litellm, store, tmp_path):
    c, inst = e2e(mod, fake_litellm, store, tmp_path, 8)
    m = {k: v.value for k, v in verdict_check(mod, c, store, inst).metadata.items()}
    assert m["verdict"] == "insufficient_evidence" and m["diff"] == "n/a" and m["n_control"] + m["n_candidate"] == 8


def test_the_check_exists_only_with_an_experiment_and_receipts(mod):
    names = lambda c: {k.name for ch in c.build_defs(None).asset_checks for k in ch.check_keys}  # noqa: E731
    assert names(comp(mod, experiments=exp(), receipts={"resource_key": KEY})) == {"experiment_verdict"}
    assert names(comp(mod, experiments=exp())) == set()
    assert names(comp(mod, receipts={"resource_key": KEY})) == set()
    assert names(comp(mod, experiments=exp(), receipts={"resource_key": KEY}, per_step_ops=True)) == {"experiment_verdict"}


def test_a_store_without_outcome_samples_reports_insufficient_evidence(mod, fake_litellm, store):
    class Old:
        def record_retrieval(self, **kw):
            return "r"
    c = comp(mod, experiments=exp(), receipts={"resource_key": KEY})
    ev = verdict_check(mod, c, Old(), dg.DagsterInstance.ephemeral())
    assert ev.metadata["verdict"].value == "insufficient_evidence" and "outcome_samples_by_code_version" in ev.metadata["reason"].value


# ── validation ────────────────────────────────────────────────────────────────

def bad(mod, msg, **kw):
    with pytest.raises(ValueError, match=msg):
        comp(mod, **kw).build_defs(None)


def test_experiment_validation(mod):
    R = {"resource_key": KEY}
    bad(mod, "only one experiment", experiments=exp() + exp(), receipts=R)
    bad(mod, "unknown key", experiments=exp(bogus=1))
    bad(mod, "name is required", experiments=[{**exp()[0], "name": "a:b"}])
    bad(mod, "unknown step id", experiments=exp(steps=["nope"]))
    bad(mod, "makes no LLM call", steps=[dict(DRAFT), {"id": "gate", "op": "approve", "approval_dir": "/tmp/x", "source": "draft"}],
        outputs={"assets": ["draft", "gate"]}, experiments=exp(steps=["gate"], variants={"w": {"gate": {"model": "x"}}}))
    bad(mod, "nested config", steps=[{"id": "b", "op": "best_of_n", "model": None, "verifier": {"model": "m"}, "n": 2}],
        outputs={"assets": ["b"]}, experiments=[{"name": "n", "steps": ["b"], "variants": {"w": {"b": {"model": "x"}}}, "fraction": 0.5}])
    bad(mod, "exactly one candidate", experiments=exp(variants={"a": CAND, "b": CAND}))
    bad(mod, "not listed in `steps`", experiments=exp(variants={"w": {"draft": {"model": "x"}}}))
    bad(mod, "cannot override", experiments=exp(variants={"w": {"polish": {"tools": []}}}))
    bad(mod, "non-empty string", experiments=exp(variants={"w": {"polish": {"model": ""}}}))
    bad(mod, "temperature", experiments=exp(variants={"w": {"polish": {"temperature": -1}}}))
    bad(mod, "does not change the prompt version", experiments=exp(variants={"w": {"polish": {"model": "gpt-4o-mini"}}}))
    bad(mod, "does not change the prompt version", experiments=exp(variants={"w": {"polish": {"system": "Be terse"}}}))
    bad(mod, "fraction", experiments=exp(fraction=0))
    bad(mod, "fraction", experiments=exp(fraction=1.5))
    bad(mod, "unit is 'partition'", experiments=exp(unit="partition"), partitions=None)
    bad(mod, "unit must be", experiments=exp(unit="week"))
    bad(mod, "persona 'ghost'", experiments=exp(variants={"w": {"polish": {"persona": "ghost"}}}))
    bad(mod, "resamples", experiments=exp(resamples=5))
    bad(mod, "promote_when", experiments=exp(promote_when={"x": 1}))


def test_receipts_validation(mod):
    bad(mod, "resource_key is required", receipts={})
    bad(mod, "unknown key", receipts={"resource_key": KEY, "context_name": "x"})
    bad(mod, "fail_on_error", receipts={"resource_key": KEY, "fail_on_error": "yes"})
    bad(mod, "unknown key", receipts={"resource_key": KEY, "outcome_signals": {"thumbs": True}})
    bad(mod, r"\[-1, 1\]", receipts={"resource_key": KEY, "outcome_signals": {"approve": {"approved": 3}}})
    bad(mod, "min must be below max", receipts={"resource_key": KEY, "outcome_signals": {"eval": {"min": 3, "max": 3}}})


def test_varying_the_judge_is_rejected_when_its_result_is_the_signal(mod):
    steps = [dict(DRAFT), {"id": "ver", "op": "verify", "source": "draft", "evidence_source": "draft", "model": "m"}]
    ex = [{"name": "j", "steps": ["ver"], "variants": {"w": {"ver": {"model": "m2"}}}, "fraction": 0.5}]
    bad(mod, "circular", steps=steps, outputs={"assets": ["draft", "ver"]}, experiments=ex, receipts={"resource_key": KEY, "outcome_signals": True})
    comp(mod, steps=steps, outputs={"assets": ["draft", "ver"]}, experiments=ex, receipts={"resource_key": KEY}).build_defs(None)   # fine without the signal


def test_unit_run_assigns_per_run(mod, fake_litellm, store):
    script(fake_litellm, lambda kw: "ok")
    c = comp(mod, partitions=None, experiments=exp(unit="run", fraction=0.5, seed=1), receipts={"resource_key": KEY})
    variants = {md(run(c, store))["agentic_experiment_variant"] for _ in range(12)}
    assert variants == {"control", "warm"}


# ── drift: the inlined bootstrap comparison is the rag_pipeline one, byte for byte ──────────────

def _segments(path, names):
    src = pathlib.Path(path).read_text()
    out = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            out[node.name] = ast.get_source_segment(src, node)
    return out


def test_compare_recipes_block_is_identical_to_rag_pipelines():
    names = {"_bootstrap_diffs", "_subsample", "_percentile_ci", "compare_recipes"}
    mine = _segments(pathlib.Path(__file__).resolve().parent.parent / "component.py", names)
    theirs = _segments(RAG_PY, names)
    assert set(mine) == names and set(theirs) == names
    for n in sorted(names):
        assert mine[n] == theirs[n], f"{n} drifted from assets/ai/rag_pipeline/component.py"
    text = (pathlib.Path(__file__).resolve().parent.parent / "component.py").read_text()
    block = text.split("# BEGIN compare-recipes", 1)[1].split("# END compare-recipes", 1)[0]
    assert all(f"def {n}(" in block for n in names)          # and they live between the markers
