"""Real, committed tests for LambdaInvokeAssetComponent.

Mocks only the external call (a fake boto3 lambda client's `.invoke()`,
matching the real response shape verified against the botocore reference:
StatusCode/Payload (a StreamingBody)/FunctionError/LogResult) -- everything
else (payload templating, response parsing, error handling, Dagster asset
wiring) is real.
"""
import io
import json

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _FakeStreamingBody:
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self):
        return self._buf.read()


class FakeLambdaClient:
    def __init__(self, response=None, raise_on_invoke=None):
        self.calls = []
        self._response = response or {}
        self._raise_on_invoke = raise_on_invoke

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        if self._raise_on_invoke:
            raise self._raise_on_invoke
        return self._response


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_lambda_client", lambda region_name, aws_profile: client)


def _comp(**kw):
    args = dict(asset_name="out", function_name="my-func", region_name="us-east-1", payload={"x": 1})
    args.update(kw)
    return mod.LambdaInvokeAssetComponent(**args)


def test_successful_synchronous_invoke_parses_json_response(monkeypatch):
    client = FakeLambdaClient(response={
        "StatusCode": 200,
        "Payload": _FakeStreamingBody(json.dumps({"result": 42}).encode()),
    })
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["status_code"].value == 200
    assert meta["response_payload"].data == {"result": 42}

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["FunctionName"] == "my-func"
    assert call["InvocationType"] == "RequestResponse"
    assert json.loads(call["Payload"]) == {"x": 1}
    assert "Qualifier" not in call


def test_function_error_raises_by_default(monkeypatch):
    client = FakeLambdaClient(response={
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": _FakeStreamingBody(json.dumps({"errorMessage": "boom", "errorType": "ValueError"}).encode()),
    })
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "boom" in str(failure.event_specific_data.error)


def test_function_error_does_not_raise_when_disabled(monkeypatch):
    client = FakeLambdaClient(response={
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": _FakeStreamingBody(json.dumps({"errorMessage": "boom"}).encode()),
    })
    _patch_client(monkeypatch, client)
    comp = _comp(fail_on_function_error=False)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["function_error"].value == "Unhandled"


def test_event_invocation_type_sends_no_payload_parsing(monkeypatch):
    client = FakeLambdaClient(response={"StatusCode": 202, "Payload": _FakeStreamingBody(b"")})
    _patch_client(monkeypatch, client)
    comp = _comp(invocation_type="Event")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    assert client.calls[0]["InvocationType"] == "Event"


def test_qualifier_is_passed_through(monkeypatch):
    client = FakeLambdaClient(response={"StatusCode": 200, "Payload": _FakeStreamingBody(b"{}")})
    _patch_client(monkeypatch, client)
    comp = _comp(qualifier="PROD")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert client.calls[0]["Qualifier"] == "PROD"


def test_payload_template_renders_partition_key(monkeypatch):
    client = FakeLambdaClient(response={"StatusCode": 200, "Payload": _FakeStreamingBody(b"{}")})
    _patch_client(monkeypatch, client)
    comp = _comp(
        payload=None,
        payload_template='{"day": "{{ partition_key }}"}',
        partition_type="daily", partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), partition_key="2024-01-02")
    assert json.loads(client.calls[0]["Payload"]) == {"day": "2024-01-02"}


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(monkeypatch):
    client = FakeLambdaClient(response={"StatusCode": 200, "Payload": _FakeStreamingBody(b"{}")})
    _patch_client(monkeypatch, client)
    comp = _comp(partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    defs = comp.build_defs(None)
    pdef = list(defs.assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
    keys = pdef.get_partition_keys(current_time=__import__("datetime").datetime(2024, 1, 10))
    assert "2024-01-06" not in keys and "2024-01-05" in keys


def test_payload_and_payload_template_together_is_rejected():
    comp = _comp(payload={"a": 1}, payload_template="{}")
    with pytest.raises(ValueError, match="payload_template"):
        comp.build_defs(None)


def test_invalid_invocation_type_is_rejected():
    comp = _comp(invocation_type="Bogus")
    with pytest.raises(ValueError, match="invocation_type"):
        comp.build_defs(None)
