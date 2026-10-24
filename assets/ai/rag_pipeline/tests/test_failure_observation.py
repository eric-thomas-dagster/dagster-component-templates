"""Failure observation contract on rag_pipeline, real Dagster; only the model call is faked.

A failing step logs an AssetObservation for the answers asset (failure_status/failure_reason/error_class/error_message/will_retry/
failed_step/run_id + rag_failures=1) and then re-raises the ORIGINAL exception."""
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module
from .test_receipts import _fake_completion

SECRET = "sk-ABCDEF0123456789abcdef0123456789"
ROWS = pd.DataFrame({"query": ["q1", "q2"]})
STEPS = [{"id": "ans", "op": "generate", "model": "gpt-4o-mini"}]
KEYS = {"failure_status", "failure_reason", "error_class", "error_message", "failed_step", "run_id", "rag_failures"}


@pytest.fixture()
def mod():
    return load_component_module()


def comp(mod, **kw):
    return mod.RAGPipelineComponent(asset_name="answers", upstream_asset_key="questions", steps=STEPS, **kw)


def run(mod, c, completion, key=None, **kw):
    inst = dg.DagsterInstance.ephemeral()

    @dg.asset(name="questions")
    def questions():
        return ROWS
    with patch("litellm.completion", side_effect=completion), patch("litellm.completion_cost", return_value=0.0):
        r = dg.materialize([questions, c.build_defs(None).assets[0]], instance=inst, partition_key=key, raise_on_error=False,
                           resources={"io_manager": dg.InMemoryIOManager()}, **kw)
    return r, inst


def chain(result):
    # [0] is Dagster's step-error wrapper; [1] is the exception the step actually raised (what propagated); deeper entries are its causes
    err = [e for e in result.all_events if e.is_failure][0].event_specific_data.error
    out = []
    while err is not None:
        out.append((err.cls_name, err.message))
        err = err.cause
    return out


def obs(result, inst):
    out = {}
    for e in inst.all_logs(result.run_id, of_type=dg.DagsterEventType.ASSET_OBSERVATION):
        o = e.dagster_event.asset_observation_data.asset_observation
        if "failure_status" in o.metadata:
            out.setdefault(o.asset_key.to_user_string(), []).append((o.partition, {k: v.value for k, v in o.metadata.items()}))
    return out


def down(**kw):
    raise ConnectionError(f"https://user:hunter2@api.example.com/v1 refused; Authorization: Bearer {SECRET}")


def test_failing_step_logs_observation_and_reraises_original(mod):
    r, inst = run(mod, comp(mod), down)
    assert not r.success
    cls, msg = chain(r)[1]
    assert cls == "ConnectionError" and "refused" in msg
    o = obs(r, inst)
    assert list(o) == ["answers"] and len(o["answers"]) == 1
    md = o["answers"][0][1]
    assert set(md) == KEYS
    assert md["failure_status"] == "failed" and md["failure_reason"] == "FAILED_TO_MATERIALIZE" and md["error_class"] == "ConnectionError"
    assert md["failed_step"] == "ans" and md["run_id"] == r.run_id and md["rag_failures"] == 1
    assert "refused" in md["error_message"] and "hunter2" not in md["error_message"] and SECRET not in md["error_message"]


def test_upstream_shape_error_is_UPSTREAM_ERROR(mod):
    c = comp(mod, query_column="missing")
    r, inst = run(mod, c, _fake_completion)
    assert not r.success and chain(r)[1][0] == "UpstreamError" and isinstance(mod.UpstreamError("x"), ValueError)
    assert obs(r, inst)["answers"][0][1]["failure_reason"] == "UPSTREAM_ERROR"


def test_partition_is_on_the_observation(mod):
    c = comp(mod, partition_type="static", partition_values=["p1", "p2"])
    inst = dg.DagsterInstance.ephemeral()

    @dg.asset(name="questions", partitions_def=dg.StaticPartitionsDefinition(["p1", "p2"]))
    def questions():
        return ROWS
    with patch("litellm.completion", side_effect=down), patch("litellm.completion_cost", return_value=0.0):
        r = dg.materialize([questions, c.build_defs(None).assets[0]], instance=inst, partition_key="p2", raise_on_error=False,
                           resources={"io_manager": dg.InMemoryIOManager()})
    assert not r.success and [p for p, _ in obs(r, inst)["answers"]] == ["p2"]


def test_success_emits_no_failure_observation(mod):
    r, inst = run(mod, comp(mod), _fake_completion)
    assert r.success and obs(r, inst) == {}


def test_emission_failure_never_masks_original(mod, monkeypatch):
    def boom(self, *a, **k):
        raise OSError("event log down")
    monkeypatch.setattr(dg.AssetExecutionContext, "log_event", boom)
    r, inst = run(mod, comp(mod), lambda **kw: (_ for _ in ()).throw(RuntimeError("the real cause")))
    assert not r.success and chain(r)[1][0] == "RuntimeError" and "the real cause" in chain(r)[1][1]
    assert not any("event log down" in m for _, m in chain(r)) and obs(r, inst) == {}


def test_will_retry_from_op_retry_policy(mod):
    r, inst = run(mod, comp(mod, retry_policy_max_retries=1, retry_policy_delay_seconds=0), down)
    assert [m["will_retry"] for _, m in obs(r, inst)["answers"]] == ["true", "false"]


def test_insights_counter_is_a_real_numeric_metadata_value(mod):
    assert set(mod._FO_COUNTER) == {"rag_failures"} and isinstance(mod._FO_COUNTER["rag_failures"], dg.IntMetadataValue)
