"""Failure observation contract on receipt_outcome_ingestion: real Dagster, real ContextReceiptStore, real files / a real refused HTTP connection."""
import dagster as dg
import pytest

from .test_receipt_outcome_ingestion import FILE_MAP, comp, md, mod, run, smod, store, write_csv  # noqa: F401

KEYS = {"failure_status", "failure_reason", "error_class", "error_message", "failed_step", "run_id", "outcome_ingestion_failures"}
KEY = "receipt_outcome_ingestion"


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


def test_failed_read_logs_observation_and_reraises_original(mod, store, tmp_path):
    result, inst = run(comp(mod, {"kind": "file", "path": str(tmp_path / "nope-*.csv")}, FILE_MAP), store)
    assert not result.success
    o = obs(result, inst)
    assert list(o) == [KEY] and len(o[KEY]) == 1
    m = o[KEY][0][1]
    assert set(m) == KEYS
    assert m["failure_status"] == "failed" and m["failure_reason"] == "FAILED_TO_MATERIALIZE"
    assert m["failed_step"] == "read_file" and m["run_id"] == result.run_id and m["outcome_ingestion_failures"] == 1
    assert chain(result)[1][0] == m["error_class"]            # the observation names the ORIGINAL exception's class


def test_failure_after_the_read_names_the_ingest_step(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "wat", "user": "u2", "ts": "2026-01-01T10:01:00Z"}])
    result, inst = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP, on_bad_row="fail"), store)
    assert not result.success and obs(result, inst)[KEY][0][1]["failed_step"] == "ingest_feedback"


def test_url_credentials_are_redacted(mod, store):
    c = comp(mod, {"kind": "http_json", "url": "http://svc-user:hunter2pw@127.0.0.1:1/feedback", "records_path": "items"}, FILE_MAP,
             retry_max_attempts=1)
    result, inst = run(c, store)
    assert not result.success
    m = obs(result, inst)[KEY][0][1]
    assert m["failed_step"] == "read_http_json"
    assert "hunter2pw" not in m["error_message"] and len(m["error_message"]) <= 500


def test_success_emits_no_failure_observation(mod, store, tmp_path):
    r = store.rids
    p = tmp_path / "fb.csv"
    write_csv(p, [{"receipt_id": r[1], "reaction": "good", "user": "u1", "ts": "2026-01-01T10:00:00Z"}])
    result, inst = run(comp(mod, {"kind": "file", "path": str(p)}, FILE_MAP), store)
    assert result.success and obs(result, inst) == {}
    assert "outcome_ingestion_failures" not in md(result)


def test_emission_failure_never_masks_original(mod, store, tmp_path, monkeypatch):
    def boom(self, *a, **k):
        raise OSError("event log down")
    monkeypatch.setattr(dg.AssetExecutionContext, "log_event", boom)
    result, inst = run(comp(mod, {"kind": "file", "path": str(tmp_path / "nope-*.csv")}, FILE_MAP), store)
    assert not result.success and not any("event log down" in m for _, m in chain(result)) and obs(result, inst) == {}


def test_will_retry_from_op_retry_policy(mod, store, tmp_path):
    c = comp(mod, {"kind": "file", "path": str(tmp_path / "nope-*.csv")}, FILE_MAP, retry_policy_max_retries=1, retry_policy_delay_seconds=0)
    result, inst = run(c, store)
    assert [m["will_retry"] for _, m in obs(result, inst)[KEY]] == ["true", "false"]


def test_insights_counter_is_a_real_numeric_metadata_value(mod):
    assert set(mod._FO_COUNTER) == {"outcome_ingestion_failures"} and isinstance(mod._FO_COUNTER["outcome_ingestion_failures"], dg.IntMetadataValue)
