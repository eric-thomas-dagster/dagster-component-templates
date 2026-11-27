"""Regression test confirming GCSResourceComponent already exposes a direct
(non-context-manager) get_client(), matching databricks_resource for
converter-friendly consistency -- no code change was needed here, only verified."""
import inspect
from unittest.mock import MagicMock, patch

from .conftest import load_component_module


def test_get_client_returns_a_direct_client_not_a_context_manager():
    mod = load_component_module()
    fake_client = MagicMock(name="storage.Client")

    with patch("google.cloud.storage.client.Client", return_value=fake_client):
        component = mod.GCSResourceComponent(project="proj")
        defs = component.build_defs(context=None)
        resource = defs.resources["gcs_resource"]

        client = resource.get_client()

        # A direct, ready client -- no `with`/`__enter__` required to use it.
        assert client is fake_client
        import dagster_gcp
        assert not inspect.isgeneratorfunction(dagster_gcp.GCSResource.get_client)
