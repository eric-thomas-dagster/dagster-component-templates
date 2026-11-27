from unittest.mock import MagicMock

import pytest

from .conftest import load_component_module

mod = load_component_module()


def _resource(**kw):
    kw.setdefault("region_name", "us-east-1")
    return mod.RedshiftDataApiResource(**kw)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod.RedshiftDataApiResource, "_client", lambda self: client)


def test_finished_statement_returns_metadata(monkeypatch):
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-1"}
    client.describe_statement.return_value = {
        "Status": "FINISHED", "Error": None, "ResultRows": 10, "ResultSize": 1024,
        "Duration": 500000000, "RedshiftQueryId": 42, "HasResultSet": True,
    }
    _patch_client(monkeypatch, client)

    result = _resource().execute_and_wait(
        sql="SELECT 1", database="db", workgroup_name="wg", poll_interval_seconds=0,
    )

    assert result["status"] == "FINISHED"
    assert result["result_rows"] == 10
    kwargs = client.execute_statement.call_args.kwargs
    assert kwargs["WorkgroupName"] == "wg"
    assert "ClusterIdentifier" not in kwargs


def test_failed_statement_raises_with_error(monkeypatch):
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-2"}
    client.describe_statement.return_value = {"Status": "FAILED", "Error": "syntax error at position 1"}
    _patch_client(monkeypatch, client)

    with pytest.raises(RuntimeError, match="syntax error"):
        _resource().execute_and_wait(sql="BAD", database="db", workgroup_name="wg", poll_interval_seconds=0)


def test_timeout_raises_before_terminal_state(monkeypatch):
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-3"}
    client.describe_statement.return_value = {"Status": "STARTED"}
    _patch_client(monkeypatch, client)

    with pytest.raises(TimeoutError):
        _resource().execute_and_wait(
            sql="SELECT 1", database="db", workgroup_name="wg",
            poll_interval_seconds=0, timeout_seconds=0,
        )


def test_requires_exactly_one_connection_mode():
    with pytest.raises(ValueError, match="exactly one"):
        _resource().execute_and_wait(sql="SELECT 1", database="db")
    with pytest.raises(ValueError, match="exactly one"):
        _resource().execute_and_wait(
            sql="SELECT 1", database="db",
            cluster_identifier="c1", workgroup_name="wg", db_user="u",
        )


def test_cluster_identifier_requires_auth():
    with pytest.raises(ValueError, match="db_user"):
        _resource().execute_and_wait(sql="SELECT 1", database="db", cluster_identifier="c1")


def test_cluster_identifier_with_db_user_passes_through(monkeypatch):
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-4"}
    client.describe_statement.return_value = {"Status": "FINISHED", "Error": None}
    _patch_client(monkeypatch, client)

    _resource().execute_and_wait(
        sql="SELECT 1", database="db", cluster_identifier="c1", db_user="u1", poll_interval_seconds=0,
    )
    kwargs = client.execute_statement.call_args.kwargs
    assert kwargs["ClusterIdentifier"] == "c1"
    assert kwargs["DbUser"] == "u1"


def test_component_registers_resource():
    component = mod.RedshiftDataApiResourceComponent(region_name="us-west-2")
    defs = component.build_defs(context=None)
    resource = defs.resources["redshift_data_api_resource"]
    assert isinstance(resource, mod.RedshiftDataApiResource)
    assert resource.region_name == "us-west-2"
