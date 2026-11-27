from unittest.mock import MagicMock

import pytest

from .conftest import load_component_module

mod = load_component_module()


def _resource(**kw):
    return mod.LambdaResource(**kw)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod.LambdaResource, "_client", lambda self: client)


def _stream(data: bytes):
    stream = MagicMock()
    stream.read.return_value = data
    return stream


def test_region_name_is_optional():
    # No region_name supplied at all -- must not raise at construction time.
    resource = _resource()
    assert resource.region_name is None


def test_invoke_round_trips_payload_and_parses_response(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": _stream(b'{"result": 42}'),
    }
    _patch_client(monkeypatch, client)

    result = _resource(region_name="us-east-1").invoke(
        function_name="my-func", payload={"key": "value"}
    )

    assert result["status_code"] == 200
    assert result["function_error"] is None
    assert result["payload"] == {"result": 42}

    client.invoke.assert_called_once()
    kwargs = client.invoke.call_args.kwargs
    assert kwargs["FunctionName"] == "my-func"
    assert kwargs["InvocationType"] == "RequestResponse"
    import json
    assert json.loads(kwargs["Payload"]) == {"key": "value"}


def test_qualifier_and_logs_are_passed_through(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": _stream(b"{}"),
        "LogResult": "aGVsbG8=",  # base64("hello")
    }
    _patch_client(monkeypatch, client)

    result = _resource().invoke(
        function_name="my-func", qualifier="PROD", include_logs=True
    )

    kwargs = client.invoke.call_args.kwargs
    assert kwargs["Qualifier"] == "PROD"
    assert kwargs["LogType"] == "Tail"
    assert result["log_tail"] == "hello"


def test_function_error_raises_by_default(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": _stream(b'{"errorMessage": "boom"}'),
    }
    _patch_client(monkeypatch, client)

    with pytest.raises(RuntimeError, match="boom"):
        _resource().invoke(function_name="my-func")


def test_function_error_can_be_tolerated(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": _stream(b'{"errorMessage": "boom"}'),
    }
    _patch_client(monkeypatch, client)

    result = _resource().invoke(function_name="my-func", fail_on_function_error=False)
    assert result["function_error"] == "Unhandled"
    assert result["payload"] == {"errorMessage": "boom"}


def test_invalid_invocation_type_raises():
    with pytest.raises(ValueError, match="invocation_type"):
        _resource().invoke(function_name="my-func", invocation_type="Bogus")


def test_invoke_payload_list_is_json_encoded(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {"StatusCode": 200, "Payload": _stream(b"{}")}
    _patch_client(monkeypatch, client)

    _resource().invoke(function_name="my-func", payload=[1, 2, "three"])

    import json
    kwargs = client.invoke.call_args.kwargs
    assert json.loads(kwargs["Payload"]) == [1, 2, "three"]


def test_invoke_payload_str_is_sent_raw_not_json_wrapped(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {"StatusCode": 200, "Payload": _stream(b"{}")}
    _patch_client(monkeypatch, client)

    _resource().invoke(function_name="my-func", payload="raw string body")

    kwargs = client.invoke.call_args.kwargs
    # Sent as-is (utf-8), NOT json.dumps("raw string body") which would add quotes.
    assert kwargs["Payload"] == b"raw string body"


def test_invoke_payload_bytes_is_passed_through_unchanged(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {"StatusCode": 200, "Payload": _stream(b"{}")}
    _patch_client(monkeypatch, client)

    raw = b"\x00\x01binary-ish"
    _resource().invoke(function_name="my-func", payload=raw)

    kwargs = client.invoke.call_args.kwargs
    assert kwargs["Payload"] == raw


def test_invoke_payload_bytearray_is_passed_through_as_bytes(monkeypatch):
    client = MagicMock()
    client.invoke.return_value = {"StatusCode": 200, "Payload": _stream(b"{}")}
    _patch_client(monkeypatch, client)

    _resource().invoke(function_name="my-func", payload=bytearray(b"ba-payload"))

    kwargs = client.invoke.call_args.kwargs
    assert kwargs["Payload"] == b"ba-payload"
    assert isinstance(kwargs["Payload"], bytes)


def test_component_registers_resource():
    component = mod.LambdaResourceComponent(region_name="us-west-2")
    defs = component.build_defs(context=None)
    resource = defs.resources["lambda_resource"]
    assert isinstance(resource, mod.LambdaResource)
    assert resource.region_name == "us-west-2"


def test_component_region_name_optional():
    component = mod.LambdaResourceComponent()
    defs = component.build_defs(context=None)
    resource = defs.resources["lambda_resource"]
    assert resource.region_name is None


def test_connection_uri_env_var_json_blob(monkeypatch):
    monkeypatch.setenv(
        "TEST_LAMBDA_BLOB",
        '{"region_name": "eu-west-1", "aws_access_key_id": "AKIA_BLOB", "aws_secret_access_key": "secret_blob"}',
    )
    component = mod.LambdaResourceComponent(connection_uri_env_var="TEST_LAMBDA_BLOB")
    defs = component.build_defs(context=None)
    resource = defs.resources["lambda_resource"]
    assert resource.region_name == "eu-west-1"
    assert resource.aws_access_key_id == "AKIA_BLOB"
    assert resource.aws_secret_access_key == "secret_blob"


def test_explicit_region_name_wins_over_connection_uri_env_var(monkeypatch):
    monkeypatch.setenv("TEST_LAMBDA_BLOB2", '{"region_name": "eu-west-1"}')
    component = mod.LambdaResourceComponent(region_name="us-east-1", connection_uri_env_var="TEST_LAMBDA_BLOB2")
    defs = component.build_defs(context=None)
    resource = defs.resources["lambda_resource"]
    assert resource.region_name == "us-east-1"
