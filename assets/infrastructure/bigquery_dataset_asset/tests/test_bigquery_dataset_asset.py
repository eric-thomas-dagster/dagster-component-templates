from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from .conftest import materialize

CREDS = {"type": "service_account", "project_id": "my-proj", "client_email": "sa@my-proj.iam.gserviceaccount.com",
         "private_key": "fake", "private_key_id": "k", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_ensure_exists_calls_create_dataset_with_exists_ok(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    created = MagicMock()
    created.location = "US"
    created.labels = {"team": "data"}
    mock_client.create_dataset.return_value = created

    component = mod.BigQueryDatasetAssetComponent(
        asset_name="bq_ds", credentials=CREDS, dataset_id="analytics", location="US",
        mode="ensure_exists", labels={"team": "data"},
    )
    result = materialize(component)
    assert result.success

    assert mock_client.create_dataset.call_count == 1
    ds_arg, kwargs = mock_client.create_dataset.call_args
    assert kwargs.get("exists_ok") is True
    assert ds_arg[0].labels == {"team": "data"}
    mock_client.delete_dataset.assert_not_called()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_recreate_deletes_then_creates(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_dataset.return_value = MagicMock(location="US", labels=None)

    component = mod.BigQueryDatasetAssetComponent(
        asset_name="bq_ds", credentials=CREDS, dataset_id="analytics", mode="recreate", delete_contents=True,
    )
    result = materialize(component)
    assert result.success

    mock_client.delete_dataset.assert_called_once()
    _, del_kwargs = mock_client.delete_dataset.call_args
    assert del_kwargs["delete_contents"] is True
    assert del_kwargs["not_found_ok"] is True
    mock_client.create_dataset.assert_called_once()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_delete_mode_only_deletes_and_reports_not_found(mock_client_cls, _mock_creds, mod):
    from google.api_core.exceptions import NotFound
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_dataset.side_effect = NotFound("gone")

    component = mod.BigQueryDatasetAssetComponent(
        asset_name="bq_ds", credentials=CREDS, dataset_id="analytics", mode="delete",
    )
    result = materialize(component)
    assert result.success

    mock_client.delete_dataset.assert_called_once()
    mock_client.create_dataset.assert_not_called()
    mats = result.asset_materializations_for_node(result.get_asset_materialization_events()[0].step_key)
    assert mats[0].metadata["exists_after_delete"].value is False


def test_requires_credentials(mod):
    import os

    component = mod.BigQueryDatasetAssetComponent(asset_name="bq_ds", dataset_id="analytics", mode="ensure_exists")
    env_backup = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    try:
        with pytest.raises(Exception, match="credentials"):
            materialize(component)
    finally:
        if env_backup is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = env_backup
