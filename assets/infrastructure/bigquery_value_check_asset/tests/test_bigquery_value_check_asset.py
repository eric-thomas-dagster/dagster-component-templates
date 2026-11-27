from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from .conftest import materialize

CREDS = {"type": "service_account", "project_id": "my-proj", "client_email": "sa@my-proj.iam.gserviceaccount.com",
         "private_key": "fake", "private_key_id": "k", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}


def _mock_query_result(mock_client, value):
    row = MagicMock()
    row.values.return_value = [value]
    job = MagicMock()
    job.result.return_value = [row]
    mock_client.query.return_value = job


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_passing_check_materializes_with_check_passed_true(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 42)

    component = mod.BigQueryValueCheckAssetComponent(
        asset_name="bq_check", credentials=CREDS, query="SELECT COUNT(*) FROM t",
        expected_value=0, comparison="greater_than",
    )
    result = materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node(result.get_asset_materialization_events()[0].step_key)
    assert mats[0].metadata["check_passed"].value is True


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_failing_check_raises_by_default(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 0)

    component = mod.BigQueryValueCheckAssetComponent(
        asset_name="bq_check", credentials=CREDS, query="SELECT COUNT(*) FROM t",
        expected_value=0, comparison="greater_than",
    )
    with pytest.raises(Exception, match="expected"):
        materialize(component)


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_fail_on_mismatch_false_materializes_anyway(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 0)

    component = mod.BigQueryValueCheckAssetComponent(
        asset_name="bq_check", credentials=CREDS, query="SELECT COUNT(*) FROM t",
        expected_value=0, comparison="greater_than", fail_on_mismatch=False,
    )
    result = materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node(result.get_asset_materialization_events()[0].step_key)
    assert mats[0].metadata["check_passed"].value is False


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_tolerance_allows_near_match(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 100.02)

    component = mod.BigQueryValueCheckAssetComponent(
        asset_name="bq_check", credentials=CREDS, query="SELECT AVG(x) FROM t",
        expected_value=100.0, comparison="equal", tolerance=0.1,
    )
    result = materialize(component)
    assert result.success


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_query_params_substitution(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 1)

    component = mod.BigQueryValueCheckAssetComponent(
        asset_name="bq_check", credentials=CREDS, query="SELECT COUNT(*) FROM `{table}`",
        query_params={"table": "my-project.analytics.orders"},
        expected_value=0, comparison="greater_than",
    )
    materialize(component)
    rendered = mock_client.query.call_args[0][0]
    assert rendered == "SELECT COUNT(*) FROM `my-project.analytics.orders`"
