"""Failure observation contract on agentic_pipeline, real Dagster, only the model call is scripted.

Open-source Dagster logs no per-asset failure event when a step raises (a failed asset is only a failed RUN), so a failing step
logs an AssetObservation per asset it was producing (failure_status/failure_reason/error_class/error_message/will_retry/failed_step/
run_id + agentic_failures=1) and then re-raises the ORIGINAL exception."""
import json
from types import SimpleNamespace

import dagster as dg
import pytest

BOTH = pytest.mark.parametrize("per_step_ops", [False, True], ids=["single_op", "per_step_ops"])
SECRET = "sk-ABCDEF0123456789abcdef0123456789"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def llm(id, source=None, **kw):
    st = {"id": id, "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "{text}", **kw}
    if source:
        st["source"] = source
    return st


def comp(mod, *, per_step_ops=False, steps=None, outputs=None, partitions=None, **kw):
    return mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "doc {partition_key}" if partitions else "doc"},
        steps=steps or [llm("a"), llm("b", source="a")], outputs=outputs or {"assets": ["a", "b"]}, per_step_ops=per_step_ops,
        **(dict(partition_type="static", partition_values=partitions) if partitions else {}), **kw)


def run(c, **kw):
    kw.setdefault("raise_on_error", False)
    inst = kw.setdefault("instance", dg.DagsterInstance.ephemeral())
    r = dg.materialize(c.build_defs(None).assets, **kw)
    r.inst = inst           # observations logged by a step that then FAILS are in the event log, not in result.all_events
    return r


def chain(result):
    # [0] is Dagster's step-error wrapper; [1] is the exception the step actually raised (what propagated); deeper entries are its causes
    """(class name, message) of the failure and every cause, outermost first."""
    err = [e for e in result.all_events if e.is_failure][0].event_specific_data.error
    out = []
    while err is not None:
        out.append((err.cls_name, err.message))
        err = err.cause
    return out


def obs(result):
    out = {}
    for e in result.inst.all_logs(result.run_id, of_type=dg.DagsterEventType.ASSET_OBSERVATION):
        o = e.dagster_event.asset_observation_data.asset_observation
        if "failure_status" in o.metadata:
            out.setdefault(o.asset_key.to_user_string(), []).append(
                (o.partition, {k: v.value for k, v in o.metadata.items()}))
    return out


def fail_model(fake, exc):
    def completion(**kw):
        raise exc
    fake.completion = completion


@BOTH
def test_failing_step_logs_observation_for_the_assets_and_reraises_the_original(mod, fake_litellm, per_step_ops):
    fail_model(fake_litellm, RuntimeError(f"model down, key {SECRET} host=x"))
    r = run(comp(mod, per_step_ops=per_step_ops))
    assert not r.success
    cls, msg = chain(r)[1]                                    # the root cause is the ORIGINAL exception, unwrapped
    assert cls == "RuntimeError" and "model down" in msg
    o = obs(r)
    assert sorted(o) == ["p_a", "p_b"]                         # nothing completed: every selected asset
    for key, evs in o.items():
        assert len(evs) == 1
        _, md = evs[0]
        assert set(md) == {"failure_status", "failure_reason", "error_class", "error_message", "failed_step", "run_id", "agentic_failures"}
        assert md["failure_status"] == "failed" and md["failure_reason"] == "FAILED_TO_MATERIALIZE"
        assert md["error_class"] == "RuntimeError" and md["agentic_failures"] == 1 and md["run_id"] == r.run_id
        assert "model down" in md["error_message"] and SECRET not in md["error_message"] and "[REDACTED]" in md["error_message"]
        assert md["failed_step"] == "a"                            # the step that raised (the same label in both build shapes)


def test_agentic_failures_is_a_real_int_metadata_value(mod, fake_litellm):
    fail_model(fake_litellm, RuntimeError("x"))
    r = run(comp(mod))
    assert isinstance(obs(r)["p_a"][0][1]["agentic_failures"], int)
    ev = r.inst.all_logs(r.run_id, of_type=dg.DagsterEventType.ASSET_OBSERVATION)[0].dagster_event.asset_observation_data.asset_observation
    assert isinstance(ev.metadata["agentic_failures"], dg.IntMetadataValue)


@BOTH
def test_partition_is_on_the_observation(mod, fake_litellm, per_step_ops):
    fail_model(fake_litellm, RuntimeError("boom"))
    r = run(comp(mod, per_step_ops=per_step_ops, partitions="x,y"), partition_key="y")
    assert not r.success
    assert {p for evs in obs(r).values() for p, _ in evs} == {"y"}


@BOTH
def test_success_emits_no_failure_observation(mod, fake_litellm, per_step_ops):
    r = run(comp(mod, per_step_ops=per_step_ops))
    assert r.success and obs(r) == {}


def test_subset_run_marks_only_the_selected_asset(mod, fake_litellm):
    fail_model(fake_litellm, RuntimeError("boom"))
    r = run(comp(mod), selection=[dg.AssetKey("p_a")])
    assert not r.success and sorted(obs(r)) == ["p_a"]


@BOTH
def test_budget_violation_is_BUDGET_EXCEEDED(mod, fake_litellm, per_step_ops):
    r = run(comp(mod, per_step_ops=per_step_ops, policy={"budget": {"max_llm_calls": 1}}))
    assert not r.success
    assert chain(r)[1][0] == "PolicyViolation"
    md = obs(r)["p_b"][0][1]
    assert md["failure_reason"] == "BUDGET_EXCEEDED" and md["error_class"] == "PolicyViolation"


@BOTH
def test_policy_violation_is_POLICY_VIOLATION(mod, fake_litellm, tmp_path, per_step_ops):
    c = comp(mod, per_step_ops=per_step_ops, outputs={"assets": ["a", "b"], "text_sinks": [{"from": "b", "path": str(tmp_path / "no" / "o.txt")}]},
             policy={"filesystem": {"write": [str(tmp_path / "ok")]}})
    r = run(c)
    assert not r.success and chain(r)[1][0] == "PolicyViolation"
    assert obs(r)["p_a"][0][1]["failure_reason"] == "POLICY_VIOLATION"


@BOTH
def test_rejected_approval_is_APPROVAL_REJECTED(mod, fake_litellm, tmp_path, per_step_ops):
    gate = {"id": "gate", "op": "approve", "source": "a", "approval_dir": str(tmp_path / "ap"), "on_timeout": "reject", "wait_seconds": 0}
    r = run(comp(mod, per_step_ops=per_step_ops, steps=[llm("a"), gate], outputs={"assets": ["a", "gate"]}))
    assert not r.success and chain(r)[1][0] == "ApprovalRejected"
    md = obs(r)["p_gate"][0][1]
    assert md["failure_reason"] == "APPROVAL_REJECTED" and md["failed_step"] in ("gate", "p_gate")
    assert isinstance(mod.ApprovalRejected("x"), RuntimeError)


@BOTH
def test_a_parked_run_is_not_a_failure(mod, fake_litellm, tmp_path, per_step_ops):
    gate = {"id": "gate", "op": "approve", "source": "a", "approval_dir": str(tmp_path / "ap"), "mode": "park", "key": "k1"}
    r = run(comp(mod, per_step_ops=per_step_ops, steps=[llm("a"), gate, llm("b", source="gate")], outputs={"assets": ["a", "gate", "b"]}),
            raise_on_error=True)
    assert r.success and obs(r) == {}


@BOTH
def test_will_retry_from_run_retry_tags(mod, fake_litellm, per_step_ops):
    fail_model(fake_litellm, RuntimeError("boom"))
    r = run(comp(mod, per_step_ops=per_step_ops), tags={"dagster/max_retries": "2", "dagster/retry_number": "0"})
    assert obs(r)["p_a"][0][1]["will_retry"] == "true"
    r = run(comp(mod, per_step_ops=per_step_ops), tags={"dagster/max_retries": "2", "dagster/retry_number": "2"})
    assert obs(r)["p_a"][0][1]["will_retry"] == "false"


@BOTH
def test_emission_failure_never_masks_the_original_exception(mod, fake_litellm, monkeypatch, per_step_ops):
    fail_model(fake_litellm, RuntimeError("the real cause"))

    def boom(self, *a, **k):
        raise OSError("event log down")
    monkeypatch.setattr(dg.AssetExecutionContext, "log_event", boom)
    monkeypatch.setattr(dg.OpExecutionContext, "log_event", boom)
    r = run(comp(mod, per_step_ops=per_step_ops))
    assert not r.success
    assert chain(r)[1][0] == "RuntimeError" and "the real cause" in chain(r)[1][1]
    assert not any("event log down" in m for _, m in chain(r))
    assert obs(r) == {}


@BOTH
def test_failure_keeps_the_original_traceback(mod, fake_litellm, per_step_ops):
    def completion(**kw):
        raise RuntimeError("deep inside the model call")
    fake_litellm.completion = completion
    r = run(comp(mod, per_step_ops=per_step_ops))
    err = [e for e in r.all_events if e.is_failure][0].event_specific_data.error
    while err.cause is not None:
        err = err.cause
    assert "in completion" in "".join(err.stack) and "deep inside the model call" in err.message


def test_insights_metric_is_listed(mod):
    assert any(m["metadata_key"] == "agentic_failures" and m["unit_type"] == "INTEGER" for m in mod.INSIGHTS_METRICS)
