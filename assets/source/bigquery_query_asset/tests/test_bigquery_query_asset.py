"""Tests for BigQueryQueryAssetComponent's destination_table/write_disposition support."""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

CREDS = {
    "type": "service_account", "project_id": "my-proj",
    "client_email": "sa@my-proj.iam.gserviceaccount.com",
    "private_key": "fake", "private_key_id": "k", "client_id": "1",
    "token_uri": "https://oauth2.googleapis.com/token",
}


@pytest.fixture()
def mod():
    return load_component_module()


def _mock_job(mock_client, df=None):
    job = MagicMock()
    job.to_dataframe.return_value = df if df is not None else pd.DataFrame({"a": [1, 2]})
    job.total_bytes_processed = 100
    job.total_bytes_billed = 100
    job.cache_hit = False
    job.slot_millis = 10
    mock_client.query.return_value = job
    return job


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_no_destination_table_leaves_job_config_destination_unset(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_job(mock_client)

    component = mod.BigQueryQueryAssetComponent(
        asset_name="q", credentials=CREDS, query="SELECT 1",
    )
    result = _materialize(component)
    assert result.success

    _, kwargs = mock_client.query.call_args
    job_config = kwargs["job_config"]
    assert job_config.destination is None

    out_metadata = result.asset_materializations_for_node("q")[0].metadata
    assert "destination_table" not in out_metadata
    assert "write_disposition" not in out_metadata


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_destination_table_sets_job_config_destination_and_default_write_disposition(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_job(mock_client)

    component = mod.BigQueryQueryAssetComponent(
        asset_name="q", credentials=CREDS, query="SELECT 1",
        destination_table="analytics.results",
    )
    result = _materialize(component)
    assert result.success

    _, kwargs = mock_client.query.call_args
    job_config = kwargs["job_config"]
    assert str(job_config.destination) == "my-proj.analytics.results"
    assert job_config.write_disposition == "WRITE_EMPTY"

    out_metadata = result.asset_materializations_for_node("q")[0].metadata
    assert out_metadata["destination_table"].text == "my-proj.analytics.results"
    assert out_metadata["write_disposition"].text == "WRITE_EMPTY"


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_write_disposition_truncate_is_passed_through(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_job(mock_client)

    component = mod.BigQueryQueryAssetComponent(
        asset_name="q", credentials=CREDS, query="SELECT 1",
        destination_table="proj2.analytics.results", write_disposition="WRITE_TRUNCATE",
    )
    result = _materialize(component)
    assert result.success

    _, kwargs = mock_client.query.call_args
    job_config = kwargs["job_config"]
    assert str(job_config.destination) == "proj2.analytics.results"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
