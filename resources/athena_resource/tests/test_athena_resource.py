from unittest.mock import MagicMock

import pytest

from .conftest import load_component_module

mod = load_component_module()


def _resource(**kw):
    kw.setdefault("region_name", "us-east-1")
    return mod.AthenaResource(**kw)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod.AthenaResource, "_client", lambda self: client)


def test_succeeded_query_returns_metadata(monkeypatch):
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid-1"}
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED", "StateChangeReason": None},
            "Statistics": {"DataScannedInBytes": 1024, "EngineExecutionTimeInMillis": 500},
            "ResultConfiguration": {"OutputLocation": "s3://bucket/qid-1.csv"},
        }
    }
    _patch_client(monkeypatch, client)

    result = _resource().run_query_and_wait(query="SELECT 1", database="db", poll_interval_seconds=0)

    assert result["state"] == "SUCCEEDED"
    assert result["data_scanned_bytes"] == 1024
    assert result["output_location"] == "s3://bucket/qid-1.csv"
    client.start_query_execution.assert_called_once()
    kwargs = client.start_query_execution.call_args.kwargs
    assert kwargs["QueryString"] == "SELECT 1"
    assert kwargs["QueryExecutionContext"] == {"Database": "db"}


def test_failed_query_raises_with_state_change_reason(monkeypatch):
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid-2"}
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "FAILED", "StateChangeReason": "SYNTAX_ERROR: bad query"},
            "Statistics": {},
            "ResultConfiguration": {},
        }
    }
    _patch_client(monkeypatch, client)

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        _resource().run_query_and_wait(query="BAD SQL", poll_interval_seconds=0)


def test_polls_until_terminal_state(monkeypatch):
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid-3"}
    client.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}, "Statistics": {}, "ResultConfiguration": {}}},
        {"QueryExecution": {"Status": {"State": "RUNNING"}, "Statistics": {}, "ResultConfiguration": {}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED", "StateChangeReason": None}, "Statistics": {}, "ResultConfiguration": {}}},
    ]
    _patch_client(monkeypatch, client)

    result = _resource().run_query_and_wait(query="SELECT 1", poll_interval_seconds=0)

    assert result["state"] == "SUCCEEDED"
    assert client.get_query_execution.call_count == 3


def test_timeout_raises_before_terminal_state(monkeypatch):
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid-4"}
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "RUNNING"}, "Statistics": {}, "ResultConfiguration": {}}
    }
    _patch_client(monkeypatch, client)

    with pytest.raises(TimeoutError):
        _resource().run_query_and_wait(query="SELECT 1", poll_interval_seconds=0, timeout_seconds=0)


def test_component_registers_resource():
    component = mod.AthenaResourceComponent(region_name="us-west-2")
    defs = component.build_defs(context=None)
    resource = defs.resources["athena_resource"]
    assert isinstance(resource, mod.AthenaResource)
    assert resource.region_name == "us-west-2"
