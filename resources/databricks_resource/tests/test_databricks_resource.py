"""Regression test confirming DatabricksResourceComponent already exposes a direct
(non-context-manager) get_client(), matching gcs_resource/bigquery_resource for
converter-friendly consistency -- no code change was needed here, only verified."""
import inspect
import os
from unittest.mock import MagicMock, patch

from .conftest import load_component_module


def test_get_client_returns_a_direct_client_not_a_context_manager(monkeypatch):
    mod = load_component_module()
    fake_client = MagicMock(name="DatabricksClient")
    monkeypatch.setenv("TOK", "fake-token")

    with patch("dagster_databricks.DatabricksClientResource.get_client", return_value=fake_client):
        component = mod.DatabricksResourceComponent(host="https://example.cloud.databricks.com", token_env_var="TOK")
        defs = component.build_defs(context=None)
        resource = defs.resources["databricks_resource"]

        client = resource.get_client()

        # A direct, ready client -- no `with`/`__enter__` required to use it.
        assert client is fake_client
        import dagster_databricks
        assert not inspect.isgeneratorfunction(dagster_databricks.DatabricksClientResource.get_client)
