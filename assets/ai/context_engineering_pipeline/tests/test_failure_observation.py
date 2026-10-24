"""Failure observation contract on context_engineering_pipeline: python, sql, and both per_step_ops shapes, real Dagster.

Open-source Dagster logs no per-asset failure event when a step raises, so the failing step logs an AssetObservation per asset it was
producing (failure_status/failure_reason/error_class/error_message/will_retry/failed_step/run_id + context_pipeline_failures=1) and
then re-raises the ORIGINAL exception. Only the model / warehouse failure is faked."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module
from .test_sql_incremental import Harness

SECRET = "ghp_" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"


@pytest.fixture()
def mod():
    return load_component_module()


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


DF = pd.DataFrame({"id": ["a", "b"], "body": ["alpha beta gamma delta", "epsilon zeta eta theta"]})
KEYS = {"failure_status", "failure_reason", "error_class", "error_message", "failed_step", "run_id", "context_pipeline_failures"}


def py_comp(mod, **kw):
    base = dict(asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
                steps=[{"id": "chunks", "op": "chunk", "chunk_size": 200}, {"id": "classified", "op": "classify", "candidate_labels": ["x", "y"]}])
    base.update(kw)
    return mod.ContextEngineeringPipelineComponent(**base)


def py_run(c, inst=None, **kw):
    inst = inst or dg.DagsterInstance.ephemeral()

    @dg.asset(name="raw")
    def _raw():
        return DF
    defs = list(c.build_defs(None).assets)
    r = dg.materialize([_raw] + defs, instance=inst, raise_on_error=False, **kw)
    return r, inst


def break_classify(mod, monkeypatch, msg=f"model down token={SECRET}"):
    def boom(df, step, context):
        raise RuntimeError(msg)
    monkeypatch.setitem(mod._FRAME_OPS, "classify", boom)


def test_python_mode_failed_step_logs_observation_and_reraises_original(mod, monkeypatch):
    break_classify(mod, monkeypatch)
    r, inst = py_run(py_comp(mod))
    assert not r.success
    cls, msg = chain(r)[1]
    assert cls == "RuntimeError" and "model down" in msg
    o = obs(r, inst)
    assert list(o) == ["kb"] and len(o["kb"]) == 1
    md = o["kb"][0][1]
    assert set(md) == KEYS
    assert md["failure_status"] == "failed" and md["failure_reason"] == "FAILED_TO_MATERIALIZE" and md["error_class"] == "RuntimeError"
    assert md["failed_step"] == "classified" and md["run_id"] == r.run_id and md["context_pipeline_failures"] == 1
    assert "model down" in md["error_message"] and SECRET not in md["error_message"] and "[REDACTED]" in md["error_message"]


def test_python_mode_upstream_shape_error_is_UPSTREAM_ERROR(mod):
    r, inst = py_run(py_comp(mod, id_column="nope"))
    assert not r.success and chain(r)[1][0] == "UpstreamError"
    assert isinstance(mod.UpstreamError("x"), ValueError)
    md = obs(r, inst)["kb"][0][1]
    assert md["failure_reason"] == "UPSTREAM_ERROR" and md["failed_step"] == "ingest"


def test_python_mode_success_emits_none(mod):
    r, inst = py_run(py_comp(mod, steps=[{"id": "chunks", "op": "chunk", "chunk_size": 200}]))
    assert r.success and obs(r, inst) == {}


def test_python_mode_partition_is_on_the_observation(mod, monkeypatch):
    break_classify(mod, monkeypatch)
    inst = dg.DagsterInstance.ephemeral()

    @dg.asset(name="raw")
    def _raw():
        return DF
    assert dg.materialize([_raw], instance=inst).success
    c = py_comp(mod, partition_type="static", partition_values=["p1", "p2"])
    r = dg.materialize([_raw] + list(c.build_defs(None).assets), instance=inst, partition_key="p2", raise_on_error=False, selection=["kb"])
    assert not r.success
    assert [p for p, _ in obs(r, inst)["kb"]] == ["p2"]


def test_python_mode_emission_failure_never_masks_original(mod, monkeypatch):
    break_classify(mod, monkeypatch, "the real cause")

    def boom(self, *a, **k):
        raise OSError("event log down")
    monkeypatch.setattr(dg.AssetExecutionContext, "log_event", boom)
    r, inst = py_run(py_comp(mod))
    assert not r.success
    assert chain(r)[1][0] == "RuntimeError" and "the real cause" in chain(r)[1][1]
    assert not any("event log down" in m for _, m in chain(r)) and obs(r, inst) == {}


def test_python_mode_will_retry_from_op_retry_policy(mod, monkeypatch):
    break_classify(mod, monkeypatch)
    r, inst = py_run(py_comp(mod, retry_policy_max_retries=1, retry_policy_delay_seconds=0))
    evs = obs(r, inst)["kb"]
    assert [m["will_retry"] for _, m in evs] == ["true", "false"]     # attempt 0 will be retried, attempt 1 is the last


def test_per_step_python_failure_marks_the_failing_steps_asset_only(mod, monkeypatch):
    break_classify(mod, monkeypatch)
    c = py_comp(mod, per_step_ops=True, steps=[{"id": "chunks", "op": "chunk", "chunk_size": 200},
                                              {"id": "classified", "op": "classify", "candidate_labels": ["x", "y"]},
                                              {"id": "again", "op": "chunk", "source": "classified"}])
    r, inst = py_run(c)
    assert not r.success and chain(r)[1][0] == "RuntimeError" and "model down" in chain(r)[1][1]   # the original, not a replacement
    o = obs(r, inst)
    assert list(o) == ["kb_classified"]                  # the step that raised; `kb` (a later step) never ran and is NOT marked; kb_chunks completed
    md = o["kb_classified"][0][1]
    assert md["failed_step"] == "classified" and set(md) == KEYS


def test_per_step_python_success_emits_none(mod):
    r, inst = py_run(py_comp(mod, per_step_ops=True, steps=[{"id": "chunks", "op": "chunk", "chunk_size": 200}]))
    assert r.success and obs(r, inst) == {}


# -- sql ------------------------------------------------------------------------------------------------------------------------

@pytest.fixture()
def h(mod):
    harness = Harness(mod, "bigquery")
    yield harness
    harness.wh.close()


def sql_run(h, **kw):
    inst = dg.DagsterInstance.ephemeral()
    defs = list(h.component(**kw).build_defs(None).assets)
    r = dg.materialize(defs, resources={"wh": h.resource}, instance=inst, raise_on_error=False)
    return r, inst


def test_sql_mode_warehouse_failure(h):
    h.con.execute("DROP TABLE ds.src")                    # the source table is gone: a real warehouse error
    r, inst = sql_run(h)
    assert not r.success
    o = obs(r, inst)
    assert list(o) == ["kb"]
    md = o["kb"][0][1]
    assert md["failure_reason"] == "FAILED_TO_MATERIALIZE" and md["failed_step"] == "sql_mode" and md["context_pipeline_failures"] == 1
    assert set(md) == KEYS and chain(r)[1][0] == md["error_class"]


def test_sql_mode_success_emits_none(h):
    r, inst = sql_run(h)
    assert r.success and obs(r, inst) == {}


def test_sql_per_step_failure_marks_the_failing_stage(h, mod, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("classify stage down")
    monkeypatch.setattr(mod, "_run_sql_feature_stage", boom)
    r, inst = sql_run(h, per_step_ops=True)
    assert not r.success and "classify stage down" in chain(r)[1][1]
    o = obs(r, inst)
    assert list(o) == ["kb_k"]                            # chunk (kb_c) completed; the embed stage (kb) never ran: not marked
    assert o["kb_k"][0][1]["failed_step"] == "k"


def test_sql_per_step_first_stage_failure(h):
    h.con.execute("DROP TABLE ds.src")
    r, inst = sql_run(h, per_step_ops=True)
    assert not r.success and list(obs(r, inst)) == ["kb_c"]


def test_sql_per_step_success_emits_none(h):
    r, inst = sql_run(h, per_step_ops=True)
    assert r.success and obs(r, inst) == {}


def test_insights_counter_is_a_real_numeric_metadata_value(mod):
    assert set(mod._FO_COUNTER) == {"context_pipeline_failures"}
    assert isinstance(mod._FO_COUNTER["context_pipeline_failures"], dg.IntMetadataValue)
