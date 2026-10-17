import importlib.util
import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
ROOT = Path(__file__).parents[1]
SCHEMA = json.loads((ROOT / "receipt-spec.v1.json").read_text())


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_spec", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


def _valid(event):
    jsonschema.Draft202012Validator(SCHEMA).validate(event)


def _full_receipt_events(rid="01RECEIPT"):
    return [
        {"spec_version": "1", "type": "retrieval", "receipt_id": rid, "context_name": "support",
         "context_version": "mat-1", "policy_version": "p1", "caller_id": "agent", "purpose": "answer",
         "query_hash": "abc", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
         "mcp_session_id": "sess-1", "mcp_request_id": "req-7", "eligible_count": 5, "excluded_count": 1,
         "items": [{"item_id": "a", "source_asset_key": "docs", "source_version": "v1", "rank": 0,
                    "score": 0.9, "selection": "ranked"},
                   {"item_id": "b", "rank": 1, "selection": "explore"}],
         "excluded": [{"item_id": "c", "reason": "pii", "source_asset_key": "docs"}]},
        {"spec_version": "1", "type": "output", "receipt_id": rid, "output_id": "o1", "model": "m",
         "output_hash": "h", "prefect_run_id": "pf-1"},
        {"spec_version": "1", "type": "evidence", "receipt_id": rid, "level": "verified",
         "details": {"item_ids": ["a"]}},
        {"spec_version": "1", "type": "outcome", "receipt_id": rid, "signal": "thumbs_down",
         "source": "support-ui", "outcome_ref": "user-1"},
    ]


def test_example_events_conform_to_schema():
    for event in _full_receipt_events():
        _valid(event)


def test_schema_rejects_content_fields_and_unknown_versions():
    bad = _full_receipt_events()[0]
    for patch in ({"query": "raw text"}, {"spec_version": "2"}, {"items": [{"item_id": "a", "text": "raw chunk"}]}):
        with pytest.raises(jsonschema.ValidationError):
            _valid({**bad, **patch})
    with pytest.raises(jsonschema.ValidationError):
        _valid({"spec_version": "1", "type": "outcome", "receipt_id": "r", "signal": "x", "value": 5})


def test_ingest_export_round_trip_and_exports_conform(mod, tmp_path):
    a = mod.ContextReceiptStore(str(tmp_path / "a.db"))
    for event in _full_receipt_events():
        assert a.ingest(event) == "01RECEIPT"
    exported = a.export_events("01RECEIPT")
    assert [e["type"] for e in exported] == ["retrieval", "output", "evidence", "outcome"]
    for event in exported:
        _valid(event)  # what we emit conforms to the published spec
    b = mod.ContextReceiptStore(str(tmp_path / "b.db"))
    for event in exported:  # replicate into a second store (migration / backend swap)
        b.ingest(event)
    strip = lambda evs: [{k: v for k, v in e.items() if k not in ("created_at", "occurred_at")} for e in evs]
    assert strip(b.export_events("01RECEIPT")) == strip(exported)
    # explore marker and evidence scoping survived the trip, so scoring is identical
    a.refresh_item_scores(); b.refresh_item_scores()
    prior = lambda st: {k: v["prior"] for k, v in st.get_item_scores(["a", "b"]).items()}
    assert prior(a) == prior(b) and prior(a)["a"] < 0
    assert {i["item_id"]: i["selection"] for i in b.get_receipt("01RECEIPT")["items"]}["b"] == "explore"


def test_ingest_rejects_bad_events(mod, tmp_path):
    s = mod.ContextReceiptStore(str(tmp_path / "x.db"))
    ok = _full_receipt_events()[0]
    with pytest.raises(ValueError, match="spec_version"):
        s.ingest({**ok, "spec_version": "2"})
    with pytest.raises(ValueError, match="unknown event type"):
        s.ingest({"spec_version": "1", "type": "mystery", "receipt_id": "r"})
    with pytest.raises(ValueError, match="unsupported retrieval field"):
        s.ingest({**ok, "raw_query": "nope"})
    s.ingest(ok)
    with pytest.raises(mod.DuplicateReceiptError):
        s.ingest(ok)
    with pytest.raises(KeyError):
        s.ingest({"spec_version": "1", "type": "outcome", "receipt_id": "ghost", "signal": "thumbs_up"})


def test_async_writer_ingest_is_idempotent_on_resend(mod, tmp_path):
    s = mod.ContextReceiptStore(str(tmp_path / "y.db"))
    w = mod.AsyncReceiptWriter(s)
    events = _full_receipt_events("01ASYNC")
    for event in events + [events[0]]:  # at-least-once delivery: retrieval re-sent
        w.ingest(event)
    w.flush()
    assert not w.dead_letters
    assert len(s.export_events("01ASYNC")) == 4


def test_trace_ids_round_trip(mod, tmp_path):
    a = mod.ContextReceiptStore(str(tmp_path / "t.db"))
    a.ingest(_full_receipt_events("01TRACE")[0])
    r = a.get_receipt("01TRACE")
    assert (r["trace_id"], r["mcp_session_id"], r["mcp_request_id"]) == (
        "4bf92f3577b34da6a3ce929d0e0e4736", "sess-1", "req-7")
    _valid(a.export_events("01TRACE")[0])
