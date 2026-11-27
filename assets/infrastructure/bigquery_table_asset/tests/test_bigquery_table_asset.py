from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from .conftest import materialize

CREDS = {"type": "service_account", "project_id": "my-proj", "client_email": "sa@my-proj.iam.gserviceaccount.com",
         "private_key": "fake", "private_key_id": "k", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}

SCHEMA = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "created", "type": "DATE"},
]


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_ensure_exists_builds_schema_fields_and_partitioning(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    field_id, field_created = MagicMock(), MagicMock()
    field_id.name = "id"
    field_created.name = "created"
    created = MagicMock()
    created.schema = [field_id, field_created]
    created.time_partitioning = MagicMock(field="created")
    mock_client.create_table.return_value = created

    component = mod.BigQueryTableAssetComponent(
        asset_name="bq_tbl", credentials=CREDS, dataset_id="analytics", table_id="orders",
        mode="ensure_exists", table_schema=SCHEMA, table_partition_field="created", cluster_fields=["id"],
    )
    result = materialize(component)
    assert result.success

    mock_client.create_table.assert_called_once()
    (table_arg,), kwargs = mock_client.create_table.call_args
    assert kwargs.get("exists_ok") is True
    assert [f.name for f in table_arg.schema] == ["id", "created"]
    assert table_arg.clustering_fields == ["id"]
    assert table_arg.time_partitioning.field == "created"
    mock_client.delete_table.assert_not_called()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_recreate_deletes_then_creates(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_table.return_value = MagicMock(schema=[], time_partitioning=None)

    component = mod.BigQueryTableAssetComponent(
        asset_name="bq_tbl", credentials=CREDS, dataset_id="analytics", table_id="orders",
        mode="recreate", table_schema=SCHEMA,
    )
    result = materialize(component)
    assert result.success

    mock_client.delete_table.assert_called_once()
    _, del_kwargs = mock_client.delete_table.call_args
    assert del_kwargs["not_found_ok"] is True
    mock_client.create_table.assert_called_once()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_delete_mode_does_not_require_schema(mock_client_cls, _mock_creds, mod):
    from google.api_core.exceptions import NotFound
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_table.side_effect = NotFound("gone")

    component = mod.BigQueryTableAssetComponent(
        asset_name="bq_tbl", credentials=CREDS, dataset_id="analytics", table_id="orders", mode="delete",
    )
    result = materialize(component)
    assert result.success
    mock_client.delete_table.assert_called_once()
    mock_client.create_table.assert_not_called()


def test_ensure_exists_requires_schema(mod):
    with pytest.raises(Exception, match="schema"):
        mod.BigQueryTableAssetComponent(
            asset_name="bq_tbl", credentials=CREDS, dataset_id="analytics", table_id="orders", mode="ensure_exists",
        )
