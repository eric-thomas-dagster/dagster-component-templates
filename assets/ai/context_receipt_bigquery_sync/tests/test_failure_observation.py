"""Failure observation contract on context_receipt_bigquery_sync: real Dagster, real ContextReceiptStore, DuckDB-backed fake BigQuery."""
import dagster as dg
import pytest

from .test_sync import component, seed
from .bq_fake import FakeBigQueryResource

SECRET = "AKIA" + "ABCDEFGHIJKLMNOP"
KEYS = {"failure_status", "failure_reason", "error_class", "error_message", "failed_step", "run_id", "receipts_sync_failures"}


class Boom(Exception):
    pass


def run(comp, store, client, **kw):
    inst = dg.DagsterInstance.ephemeral()
    defs = component(comp, **kw).build_defs(None)
    r = dg.materialize(list(defs.assets), resources={"receipts": store, "bigquery": FakeBigQueryResource(client)}, instance=inst,
                       raise_on_error=False)
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


def crash_state(comp, monkeypatch, msg):
    def crash(*a, **k):
        raise Boom(msg)
    monkeypatch.setattr(comp, "_write_state", crash)


def test_failed_sync_logs_observation_and_reraises_original(comp, store, bq, monkeypatch):
    client, _ = bq
    seed(store, 2)
    crash_state(comp, monkeypatch, f"cursor write failed aws_secret_access_key={SECRET}")
    r, inst = run(comp, store, client)
    assert not r.success
    cls, msg = chain(r)[1]
    assert cls == "Boom" and "cursor write failed" in msg
    o = obs(r, inst)
    assert list(o) == ["context_receipt_bigquery_sync"] and len(o["context_receipt_bigquery_sync"]) == 1
    md = o["context_receipt_bigquery_sync"][0][1]
    assert set(md) == KEYS
    assert md["failure_status"] == "failed" and md["failure_reason"] == "FAILED_TO_MATERIALIZE" and md["error_class"] == "Boom"
    assert md["failed_step"] == "sync_receipts" and md["run_id"] == r.run_id and md["receipts_sync_failures"] == 1
    assert "cursor write failed" in md["error_message"] and SECRET not in md["error_message"]


def test_success_emits_no_failure_observation(comp, store, bq):
    client, _ = bq
    seed(store, 2)
    r, inst = run(comp, store, client)
    assert r.success and obs(r, inst) == {}


def test_emission_failure_never_masks_original(comp, store, bq, monkeypatch):
    client, _ = bq
    seed(store, 1)
    crash_state(comp, monkeypatch, "the real cause")

    def boom(self, *a, **k):
        raise OSError("event log down")
    monkeypatch.setattr(dg.AssetExecutionContext, "log_event", boom)
    r, inst = run(comp, store, client)
    assert not r.success and chain(r)[1][0] == "Boom" and "the real cause" in chain(r)[1][1]
    assert not any("event log down" in m for _, m in chain(r)) and obs(r, inst) == {}


def test_will_retry_from_op_retry_policy(comp, store, bq, monkeypatch):
    client, _ = bq
    seed(store, 1)
    crash_state(comp, monkeypatch, "x")
    r, inst = run(comp, store, client, retry_policy_max_retries=1, retry_policy_delay_seconds=0)
    assert [m["will_retry"] for _, m in obs(r, inst)["context_receipt_bigquery_sync"]] == ["true", "false"]


def test_insights_counter_is_a_real_numeric_metadata_value(comp):
    assert set(comp._FO_COUNTER) == {"receipts_sync_failures"} and isinstance(comp._FO_COUNTER["receipts_sync_failures"], dg.IntMetadataValue)
