"""park mode follow-ups: (a) the `pending_approvals` WARN asset check + `agentic_parked_runs` metadata, (b) config changed while parked
(`approve.on_config_change: fail | continue`, judged on the steps that still have to run only).

Real Dagster materializations on a real instance, a real approval store, litellm.completion faked. Asset checks only run through a job
(dg.materialize does not run them), so the check tests execute the implicit global asset job."""
import json
import pathlib

import dagster as dg
import pytest

from .test_park import BOTH, Pipe, _env, _llm, err_text, fk, md, mats  # noqa: F401  (fixtures re-used)

CHECK = "pending_approvals"


def job_run(p, key=None, tags=None, only_checks=False):
    """Run the implicit global job (assets AND checks); returns (result, {check name: AssetCheckEvaluation})."""
    if only_checks:
        job = dg.define_asset_job("chk", selection=dg.AssetSelection.checks_for_assets(dg.AssetKey("p_draft")))
        job_def = dg.Definitions.merge(p.defs, dg.Definitions(jobs=[job])).resolve_job_def("chk")
    else:
        job_def = p.defs.get_implicit_global_asset_job_def()
    r = job_def.execute_in_process(instance=p.inst, partition_key=key, tags=tags, raise_on_error=False)
    return r, {e.check_name: e for e in r.get_asset_check_evaluations()}


def job_resume(p, rr):
    return job_run(p, key=rr.partition_key, tags=rr.tags)


# ── (a) pending_approvals check ───────────────────────────────────────────────

@BOTH
def test_check_exists_only_when_a_gate_is_park_and_is_attached_to_the_first_asset_with_warn_severity(mod, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    specs = [s for c in p.defs.asset_checks for s in c.check_specs]
    assert [(s.name, s.asset_key.to_user_string()) for s in specs] == [(CHECK, "p_draft")]
    gate = {"id": "gate", "op": "approve", "source": "draft", "approval_dir": str(tmp_path / "w"), "mode": "wait", "key": "k1"}
    wait = mod.AgenticPipelineComponent(asset_name_prefix="p", source={"kind": "literal", "text": "doc"}, per_step_ops=per_step_ops,
                                        steps=[_llm("draft", "Draft: {text}"), gate], outputs={"assets": ["draft", "gate"]}).build_defs(None)
    assert [s for c in (wait.asset_checks or []) for s in c.check_specs] == []


def pa_tick(p):
    """One evaluation of the generated `<prefix>_pending_approvals_sensor`: the checks-only run it requests, or None."""
    sensor = p.defs.get_sensor_def("p_pending_approvals_sensor")
    ctx = dg.build_sensor_context(instance=p.inst, definitions=p.defs, cursor=getattr(p, "_pa_cursor", None))
    out = sensor(ctx)
    p._pa_cursor = ctx.cursor
    return None if isinstance(out, dg.SkipReason) else out


@BOTH
def test_the_check_runs_in_the_parked_run_warns_with_details_and_a_sensor_clears_it_after_the_resume(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    assert pa_tick(p) is None                                           # nothing ever parked: nothing to evaluate
    r, ev = job_run(p)
    assert r.success                                                    # the parked run itself is SUCCESS ...
    e = ev[CHECK]                                                       # ... and the check says work is pending
    assert e.passed is False and e.severity == dg.AssetCheckSeverity.WARN and e.asset_key == dg.AssetKey("p_draft")
    m = {k: v.value for k, v in e.metadata.items()}
    assert m["pending_count"] == 1 and m["waiting_approve_steps"] == {"gate": 1} and m["partitions"] == []
    assert m["oldest_age_hours"] >= 0 and len(m["checkpoint_ids"]) == 1 and m["warn_after_hours"] == 0.0
    assert pa_tick(p) is not None and pa_tick(p) is None                # the set changed once; unchanged afterwards

    p.approve()
    rr = p.tick()[0][0]
    r2, ev2 = job_resume(p, rr)
    # a resumed run does NOT re-materialize the first asset, so the check cannot run inside it: the sensor's checks-only run clears the WARN
    assert r2.success and CHECK not in ev2
    req = pa_tick(p)
    assert req is not None
    _, ev3 = job_run(p, only_checks=True)
    assert ev3[CHECK].passed is True and ev3[CHECK].metadata["pending_count"].value == 0
    assert pa_tick(p) is None


def test_warn_after_hours_threshold_and_oldest_age(mod, fk, tmp_path, monkeypatch):
    p = Pipe(mod, tmp_path)
    p.comp = p.comp.model_copy(update={"park_warn_after_hours": 24.0})
    p.defs = p.comp.build_defs(None)
    job_run(p)
    _, ev = job_run(p, only_checks=True)
    assert ev[CHECK].passed is True and ev[CHECK].metadata["pending_count"].value == 1      # parked, but not yet 24h
    real = mod._park_now
    monkeypatch.setattr(mod, "_park_now", lambda: real() + __import__("datetime").timedelta(hours=25))
    _, ev = job_run(p, only_checks=True)
    assert ev[CHECK].passed is False and ev[CHECK].severity == dg.AssetCheckSeverity.WARN
    assert 24.9 < ev[CHECK].metadata["oldest_age_hours"].value < 25.1


@BOTH
def test_partitioned_check_judges_its_own_partition_and_lists_partitions(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops, partitions="a,b,c")
    job_run(p, key="a")
    job_run(p, key="b")
    _, ev_a = job_run(p, key="a")                                       # a re-run of 'a' (still parked: superseded/overwritten, still one)
    m = {k: v.value for k, v in ev_a[CHECK].metadata.items()}
    assert ev_a[CHECK].passed is False and m["partitions"] == ["a"] and m["pending_count"] == 1 and m["pending_count_all_partitions"] == 2
    _, ev_c = job_run(p, key="c")                                       # c parks too ...
    assert ev_c[CHECK].metadata["pending_count_all_partitions"].value == 3
    p.approve("a")                                                      # ... resume only 'a'
    reqs = [r for r in p.tick()[0] if r.partition_key == "a"]
    job_resume(p, reqs[0])
    _, ev_after = job_run(p, only_checks=True)                         # what the sensor's checks-only run evaluates (no partition: all)
    assert ev_after[CHECK].passed is False and ev_after[CHECK].metadata["pending_count"].value == 2     # b and c still parked


@BOTH
def test_agentic_parked_runs_metadata_on_the_parked_runs_first_asset_and_listed_for_insights(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops, partitions="a,b")
    assert md(p.run(key="a"), "p_draft")["agentic_parked_runs"] == 1
    r = p.run(key="b")
    assert md(r, "p_draft")["agentic_parked_runs"] == 2                 # 'a' is still parked
    ev = [e for e in r.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("p_draft")][0]
    assert isinstance(ev.materialization.metadata["agentic_parked_runs"], dg.IntMetadataValue)
    assert {"metadata_key": "agentic_parked_runs", "unit_type": "INTEGER"}.items() <= next(
        m for m in mod.INSIGHTS_METRICS if m["metadata_key"] == "agentic_parked_runs").items()
    # a non-parking run never emits it
    q = Pipe(mod, tmp_path / "q", per_step_ops)
    q.run()
    q.approve()
    assert "agentic_parked_runs" not in md(q.resume(q.tick()[0][0]), "p_gate")      # a resumed (non-parking) run does not emit it


# ── (b) config changed while parked ───────────────────────────────────────────

def changed(p, tmp_path, edit, per_step_ops=False, **kw):
    """The same pipeline over the SAME approval dir after the YAML was edited: edit(steps) -> steps."""
    steps = edit([dict(s) for s in p.comp.steps])
    gate = next(s for s in steps if s["id"] == "gate")
    q = Pipe(p.mod, tmp_path, per_step_ops, steps=steps, gate={k: v for k, v in gate.items() if k not in ("id", "op")}, **kw)
    q.inst = p.inst
    return q


def edit_step(step_id, **kv):
    def f(steps):
        for s in steps:
            if s["id"] == step_id:
                s.update(kv)
        return steps
    return f


def resumable(p):
    ck = json.loads(p.ckpts()[0].read_text())
    claims = list((pathlib.Path(p.ap) / "checkpoints").glob("*.claim.*"))
    return ck["resume_count"] == 0 and not claims


@BOTH
def test_editing_a_remaining_steps_prompt_refuses_the_resume_clearly_and_leaves_the_checkpoint_resumable(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    p.run()
    p.approve()
    q = changed(p, tmp_path, edit_step("publish", prompt_template="Publish (v2): {text}"), per_step_ops)
    reqs, why = q.tick()
    assert reqs == [] and "publish" in why and "on_config_change: continue" in why       # the sensor does not even launch a doomed run
    # a run that gets launched anyway (manual / config deployed after the request) fails with the same clear message
    rr = p.tick()[0][0]
    r = q.resume(rr, raise_on_error=False)
    assert not r.success
    msg = err_text(r)
    assert "on_config_change: fail" in msg and "'publish'" in msg and "->" in msg
    assert "on_config_change: continue" in msg and "revert" in msg and "stays resumable" in msg
    assert "'draft'" not in msg and fk.n("Publish") == 0
    assert resumable(q) and q.pending() is not None                                      # untouched: no claim, no attempt consumed
    # reverting the config: the very same checkpoint resumes (the sensor gets a fresh run_key, the refused run's key is spent)
    reqs, _ = p.tick()
    assert len(reqs) == 1 and reqs[0].run_key != rr.run_key and reqs[0].run_key.endswith(":c1")
    r3 = p.resume(reqs[0])
    assert r3.success and fk.n("Publish") == 1 and p.ckpts() == []


@BOTH
def test_editing_a_step_that_already_ran_or_unrelated_config_does_not_block(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    p.run()
    p.approve()
    q = changed(p, tmp_path, edit_step("draft", prompt_template="Draft (v2): {text}"), per_step_ops)   # done by the parked run
    reqs, _ = q.tick()
    assert len(reqs) == 1
    r = q.resume(reqs[0])
    assert r.success and fk.n("Publish") == 1 and "DRAFT-v1" in md(r, "p_publish")["publish__text"]   # the checkpointed draft, not a re-run
    # budget tuning / source text are not part of any step's version either
    p2 = Pipe(mod, tmp_path / "b", per_step_ops)
    p2.run()
    p2.approve()
    q2 = changed(p2, tmp_path / "b", lambda s: s, per_step_ops)
    q2.comp = q2.comp.model_copy(update={"policy": {"budget": {"max_cost_usd": 9.0}}, "source": {"kind": "literal", "text": "other doc"}})
    q2.defs = q2.comp.build_defs(None)
    reqs, _ = q2.tick()
    assert len(reqs) == 1 and q2.resume(reqs[0]).success


@BOTH
def test_on_config_change_continue_proceeds_with_the_new_configuration_and_a_warning(mod, fk, tmp_path, per_step_ops):
    p = Pipe(mod, tmp_path, per_step_ops)
    p.run()
    p.approve()
    q = changed(p, tmp_path, lambda s: edit_step("gate", on_config_change="continue")(edit_step("publish", prompt_template="Publish (v2): {text}")(s)),
                per_step_ops)
    reqs, _ = q.tick()
    assert len(reqs) == 1
    r = q.resume(reqs[0])
    assert r.success and fk.n("Publish (v2)") == 1
    logs = " ".join(str(e.message) for e in q.inst.all_logs(r.run_id) if e.dagster_event is None)
    assert "on_config_change: continue" in logs and "'publish'" in logs and "NEW configuration" in logs


def test_flipping_the_escape_hatch_alone_is_not_a_change(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    p.approve()
    q = changed(p, tmp_path, edit_step("gate", on_config_change="fail"))
    assert len(q.tick()[0]) == 1 and q.resume(q.tick()[0][0]).success


def test_persona_edit_blocks_only_when_a_remaining_step_uses_it(mod, fk, tmp_path):
    def build(tmp, pa, pb):
        ap = str(tmp / "approvals")
        steps = [_llm("draft", "Draft: {text}", persona="pa"),
                 {"id": "gate", "op": "approve", "source": "draft", "approval_dir": ap, "mode": "park", "key": "k1"},
                 _llm("publish", "Publish: {text}", source="gate", persona="pb")]
        c = mod.AgenticPipelineComponent(asset_name_prefix="p", source={"kind": "literal", "text": "doc"}, steps=steps,
                                         outputs={"assets": ["draft", "gate", "publish"]},
                                         personas={"pa": {"temperature": pa}, "pb": {"temperature": pb}})
        return c.build_defs(None)
    inst = dg.DagsterInstance.ephemeral()
    ap = tmp_path / "approvals"

    def go(defs, tags=None):
        return dg.materialize(defs.assets, instance=inst, tags=tags, raise_on_error=False)

    def requests(defs):
        out = defs.get_sensor_def("p_approval_resume_sensor")(dg.build_sensor_context(instance=inst, definitions=defs))
        return ([] if isinstance(out, dg.SkipReason) else list(out.run_requests)), (out.skip_message if isinstance(out, dg.SkipReason) else None)

    d0 = build(tmp_path, 0.1, 0.1)
    go(d0)
    mod.approve_pending(str(ap), "k1", approver="eric")
    reqs, why = requests(build(tmp_path, 0.9, 0.1))                    # persona of the DONE step edited: fine
    assert len(reqs) == 1
    reqs, why = requests(build(tmp_path, 0.1, 0.9))                    # persona of the REMAINING step edited: blocked
    assert reqs == [] and "'publish'" in why and "'draft'" not in why


def test_a_checkpoint_from_before_step_versions_falls_back_to_the_old_warning(mod, fk, tmp_path):
    p = Pipe(mod, tmp_path)
    p.run()
    p.approve()
    f = p.ckpts()[0]
    ck = json.loads(f.read_text())
    del ck["step_versions"]
    f.write_text(json.dumps(ck))
    q = changed(p, tmp_path, edit_step("publish", prompt_template="Publish (v2): {text}"))
    reqs, _ = q.tick()
    assert len(reqs) == 1 and q.resume(reqs[0]).success                # nothing to compare: warns (logged), proceeds as before


def test_on_config_change_is_validated_and_park_only(mod, tmp_path):
    with pytest.raises(ValueError, match="on_config_change must be fail \\| continue"):
        Pipe(mod, tmp_path, gate={"on_config_change": "ignore"})
    with pytest.raises(ValueError, match="only apply to mode: park"):
        mod._validate_approve_options([{"id": "g", "op": "approve", "approval_dir": "x", "mode": "wait", "on_config_change": "continue"}])
