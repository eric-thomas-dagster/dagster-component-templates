"""Committed regression tests for BigQueryResourceComponent's get_direct_client() addition."""
from unittest.mock import MagicMock, patch

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_get_client_is_still_a_context_manager(mod):
    """The official contract (context manager) must keep working unmodified."""
    fake_client = MagicMock(name="bigquery.Client")
    with patch("google.cloud.bigquery.Client", return_value=fake_client):
        component = mod.BigQueryResourceComponent(project="proj")
        defs = component.build_defs(context=None)
        resource = defs.resources["bigquery_resource"]
        with resource.get_client() as client:
            assert client is fake_client


def test_get_direct_client_returns_a_real_client_without_a_with_block(mod):
    fake_client = MagicMock(name="bigquery.Client")
    with patch("google.cloud.bigquery.Client", return_value=fake_client):
        component = mod.BigQueryResourceComponent(project="proj", location="US")
        defs = component.build_defs(context=None)
        resource = defs.resources["bigquery_resource"]

        client = resource.get_direct_client()

        assert client is fake_client


def test_get_direct_client_passes_project_and_location(mod):
    with patch("google.cloud.bigquery.Client") as mock_client_cls:
        mock_client_cls.return_value = MagicMock()
        component = mod.BigQueryResourceComponent(project="my-proj", location="EU")
        defs = component.build_defs(context=None)
        resource = defs.resources["bigquery_resource"]

        resource.get_direct_client()

        mock_client_cls.assert_called_once_with(project="my-proj", location="EU")


def test_resource_key_is_configurable(mod):
    with patch("google.cloud.bigquery.Client", return_value=MagicMock()):
        component = mod.BigQueryResourceComponent(project="proj", resource_key="my_bq")
        defs = component.build_defs(context=None)
        assert "my_bq" in defs.resources
        assert hasattr(defs.resources["my_bq"], "get_direct_client")
