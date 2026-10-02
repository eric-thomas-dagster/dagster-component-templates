"""Committed regression tests for MetronomeUsageEventSendComponent.

The real `requests` network call is never made here -- `_call_metronome_ingest`
(the one external, paid-API boundary) is monkeypatched wholesale, while event
construction, transaction_id idempotency derivation, batching, and the
retry/backoff state machine are all exercised for real. `time.sleep` is
monkeypatched to a no-op so retry tests run instantly.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeMetronomeResource, FakeResponse, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def no_real_sleep(mod, monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_k: None)


@pytest.fixture()
def recorded_batches(mod, monkeypatch):
    calls = []

    def _fake_call(resource, events):
        calls.append(list(events))
        return FakeResponse(200, body={})

    monkeypatch.setattr(mod, "_call_metronome_ingest", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_usage", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"metronome_resource": resource})


# --- validation -------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MetronomeUsageEventSendComponent(
            asset_name="x",
            customer_id_column="cust",
            event_type="api_call",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MetronomeUsageEventSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            customer_id_column="cust",
            event_type="api_call",
        ).build_defs(context=None)


def test_neither_event_type_nor_column_raises(mod):
    with pytest.raises(ValueError, match="event_type"):
        mod.MetronomeUsageEventSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            customer_id_column="cust",
        ).build_defs(context=None)


def test_both_event_type_and_column_raises(mod):
    with pytest.raises(ValueError, match="event_type"):
        mod.MetronomeUsageEventSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            customer_id_column="cust",
            event_type="api_call",
            event_type_column="et",
        ).build_defs(context=None)


def test_batch_size_over_100_rejected(mod):
    with pytest.raises(Exception):  # pydantic ValidationError
        mod.MetronomeUsageEventSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            customer_id_column="cust",
            event_type="api_call",
            batch_size=101,
        )


def test_missing_customer_id_column_raises_failure(mod, recorded_batches):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
    )
    with pytest.raises(dg.Failure, match="customer_id_column"):
        _materialize(component, df, resource)


# --- event construction ------------------------------------------------------

def test_send_end_to_end_builds_expected_event(mod, recorded_batches):
    df = pd.DataFrame({
        "cust": ["cust_abc"],
        "called_at": ["2026-03-09T12:00:00Z"],
        "req_id": ["req-1"],
        "endpoint": ["/v1/predict"],
        "status_code": [200],
    })
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        timestamp_column="called_at",
        transaction_id_column="req_id",
        properties_columns=["endpoint", "status_code"],
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_batches) == 1
    batch = recorded_batches[0]
    assert len(batch) == 1
    event = batch[0]
    assert event["transaction_id"] == "req-1"
    assert event["customer_id"] == "cust_abc"
    assert event["event_type"] == "api_call"
    assert event["timestamp"] == "2026-03-09T12:00:00Z"
    assert event["properties"] == {"endpoint": "/v1/predict", "status_code": 200}

    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 1
    assert out["batches_sent"] == 1
    assert out["rows_skipped_no_customer_id"] == 0


def test_customer_id_accepts_ingest_alias_string(mod, recorded_batches):
    """customer_id accepts either a UUID or an ingest alias -- same field,
    this component doesn't need to distinguish them."""
    df = pd.DataFrame({"cust": ["my-internal-alias-123"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_batches[0][0]["customer_id"] == "my-internal-alias-123"


def test_blank_customer_id_skipped_and_counted(mod, recorded_batches):
    df = pd.DataFrame({"cust": ["cust_abc", None, "  ", ""]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_batches[0]) == 1
    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 1
    assert out["rows_skipped_no_customer_id"] == 3


def test_event_type_column_per_row(mod, recorded_batches):
    df = pd.DataFrame({"cust": ["c1", "c2"], "et": ["api_call", "storage_gb"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type_column="et",
    )
    result = _materialize(component, df, resource)
    assert result.success
    events = recorded_batches[0]
    assert events[0]["event_type"] == "api_call"
    assert events[1]["event_type"] == "storage_gb"


def test_properties_default_to_non_reserved_columns(mod, recorded_batches):
    df = pd.DataFrame({"cust": ["c1"], "event_type_col": ["api_call"], "foo": ["bar"], "n": [5]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type_column="event_type_col",
    )
    result = _materialize(component, df, resource)
    assert result.success
    props = recorded_batches[0][0]["properties"]
    assert props == {"foo": "bar", "n": 5}


# --- transaction_id idempotency ----------------------------------------------

def test_transaction_id_column_used_verbatim(mod, recorded_batches):
    df = pd.DataFrame({"cust": ["c1"], "rid": ["stable-id-1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        transaction_id_column="rid",
    )
    _materialize(component, df, resource)
    assert recorded_batches[0][0]["transaction_id"] == "stable-id-1"


def test_default_transaction_id_is_deterministic(mod):
    tid1 = mod._default_transaction_id("cust_abc", "api_call", "2026-01-01T00:00:00Z", 0)
    tid2 = mod._default_transaction_id("cust_abc", "api_call", "2026-01-01T00:00:00Z", 0)
    assert tid1 == tid2  # same inputs -> same transaction_id -> safe retries/reruns

    tid_diff_row = mod._default_transaction_id("cust_abc", "api_call", "2026-01-01T00:00:00Z", 1)
    assert tid_diff_row != tid1


# --- batching -----------------------------------------------------------------

def test_batch_size_chunks_requests(mod, recorded_batches):
    df = pd.DataFrame({"cust": [f"c{i}" for i in range(250)]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        batch_size=100,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_batches) == 3  # 100 + 100 + 50
    assert [len(b) for b in recorded_batches] == [100, 100, 50]
    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 250
    assert out["batches_sent"] == 3


def test_max_rows_per_run_caps_total_rows(mod, recorded_batches):
    df = pd.DataFrame({"cust": [f"c{i}" for i in range(10)]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_rows_per_run=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 3


# --- retry / backoff semantics (idempotency-critical) ------------------------

def test_5xx_retries_then_succeeds(mod, monkeypatch):
    responses = [FakeResponse(500), FakeResponse(502), FakeResponse(200, body={})]
    calls = []

    def _fake_call(resource, events):
        calls.append(list(events))
        return responses.pop(0)

    monkeypatch.setattr(mod, "_call_metronome_ingest", _fake_call)

    df = pd.DataFrame({"cust": ["c1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_retries=5,
        initial_backoff_seconds=0.001,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(calls) == 3  # 2 failures + 1 success
    # Same transaction_id sent on every retry attempt -- Metronome's dedup
    # window is what makes this safe rather than a source of double-billing.
    tids = {c[0]["transaction_id"] for c in calls}
    assert len(tids) == 1


def test_5xx_exhausts_retries_raises_failure(mod, monkeypatch):
    def _always_500(resource, events):
        return FakeResponse(500)

    monkeypatch.setattr(mod, "_call_metronome_ingest", _always_500)

    df = pd.DataFrame({"cust": ["c1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_retries=2,
        initial_backoff_seconds=0.001,
    )
    with pytest.raises(dg.Failure, match="server error"):
        _materialize(component, df, resource)


def test_network_error_retries_then_succeeds(mod, monkeypatch):
    attempts = {"n": 0}

    def _flaky(resource, events):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("boom")
        return FakeResponse(200, body={})

    monkeypatch.setattr(mod, "_call_metronome_ingest", _flaky)

    df = pd.DataFrame({"cust": ["c1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_retries=5,
        initial_backoff_seconds=0.001,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert attempts["n"] == 3


def test_429_backs_off_and_retries(mod, monkeypatch):
    responses = [FakeResponse(429, headers={"Retry-After": "0"}), FakeResponse(200, body={})]
    calls = []

    def _fake_call(resource, events):
        calls.append(events)
        return responses.pop(0)

    monkeypatch.setattr(mod, "_call_metronome_ingest", _fake_call)

    df = pd.DataFrame({"cust": ["c1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_retries=3,
        initial_backoff_seconds=0.001,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(calls) == 2


def test_non_retryable_4xx_raises_immediately(mod, monkeypatch):
    calls = []

    def _fake_call(resource, events):
        calls.append(events)
        return FakeResponse(400, text="invalid customer_id")

    monkeypatch.setattr(mod, "_call_metronome_ingest", _fake_call)

    df = pd.DataFrame({"cust": ["c1"]})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
        max_retries=5,
        initial_backoff_seconds=0.001,
    )
    with pytest.raises(dg.Failure, match="not retryable"):
        _materialize(component, df, resource)
    # Not retried -- exactly one call for a non-retryable 4xx.
    assert len(calls) == 1


# --- source inline mode -------------------------------------------------------

def test_source_inline_mode(mod, recorded_batches):
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        source={"kind": "inline", "rows": [{"cust": "c1"}, {"cust": "c2"}]},
        customer_id_column="cust",
        event_type="api_call",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"metronome_resource": resource})
    assert result.success
    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 2


def test_empty_upstream_sends_nothing(mod, recorded_batches):
    df = pd.DataFrame({"cust": []})
    resource = FakeMetronomeResource()
    component = mod.MetronomeUsageEventSendComponent(
        asset_name="metronome_out",
        upstream_asset_key="upstream_usage",
        customer_id_column="cust",
        event_type="api_call",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_batches == []
    out = metadata_for(result, "metronome_out")
    assert out["events_sent"] == 0
