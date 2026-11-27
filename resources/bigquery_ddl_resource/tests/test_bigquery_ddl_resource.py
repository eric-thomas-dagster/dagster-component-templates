from unittest.mock import MagicMock, patch

import pytest

CREDS = {"type": "service_account", "project_id": "my-proj", "client_email": "sa@my-proj.iam.gserviceaccount.com",
         "private_key": "fake", "private_key_id": "k", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}


def _mock_query_result(mock_client, value):
    row = MagicMock()
    row.values.return_value = [value]
    job = MagicMock()
    job.result.return_value = [row]
    mock_client.query.return_value = job


# --- dataset lifecycle -----------------------------------------------------

@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_ensure_dataset_calls_create_dataset_with_exists_ok(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    created = MagicMock()
    created.location = "US"
    created.labels = {"team": "data"}
    mock_client.create_dataset.return_value = created

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.ensure_dataset("analytics", location="US", labels={"team": "data"})

    assert result["dataset_id"] == "my-proj.analytics"
    assert result["labels"] == {"team": "data"}
    assert mock_client.create_dataset.call_count == 1
    ds_arg, kwargs = mock_client.create_dataset.call_args
    assert kwargs.get("exists_ok") is True
    assert ds_arg[0].labels == {"team": "data"}
    mock_client.delete_dataset.assert_not_called()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_recreate_dataset_deletes_then_creates(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_dataset.return_value = MagicMock(location="US", labels=None)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    resource.recreate_dataset("analytics", delete_contents=True)

    mock_client.delete_dataset.assert_called_once()
    _, del_kwargs = mock_client.delete_dataset.call_args
    assert del_kwargs["delete_contents"] is True
    assert del_kwargs["not_found_ok"] is True
    mock_client.create_dataset.assert_called_once()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_delete_dataset_reports_not_found_after(mock_client_cls, _mock_creds, mod):
    from google.api_core.exceptions import NotFound
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_dataset.side_effect = NotFound("gone")

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.delete_dataset("analytics")

    mock_client.delete_dataset.assert_called_once()
    mock_client.create_dataset.assert_not_called()
    assert result["exists_after_delete"] is False


def test_requires_credentials(mod):
    import os

    resource = mod.BigQueryDdlResource()
    env_backup = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    try:
        with pytest.raises(Exception, match="credentials"):
            resource.ensure_dataset("analytics")
    finally:
        if env_backup is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = env_backup


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_credentials_env_var_parsed_as_json(mock_client_cls, mock_from_info, mod, monkeypatch):
    import json

    monkeypatch.setenv("MY_BQ_CREDS", json.dumps(CREDS))
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    created = MagicMock()
    created.location = "US"
    created.labels = {}
    mock_client.create_dataset.return_value = created

    resource = mod.BigQueryDdlResource(credentials_env_var="MY_BQ_CREDS", project_id="my-proj")
    resource.ensure_dataset("analytics")

    mock_from_info.assert_called_once_with(CREDS)


def test_credentials_env_var_missing_raises(mod, monkeypatch):
    monkeypatch.delenv("MY_BQ_CREDS", raising=False)
    resource = mod.BigQueryDdlResource(credentials_env_var="MY_BQ_CREDS")
    with pytest.raises(ValueError, match="MY_BQ_CREDS"):
        resource.ensure_dataset("analytics")


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_credentials_dict_takes_precedence_over_env_var(mock_client_cls, mock_from_info, mod, monkeypatch):
    import json

    other_creds = dict(CREDS, project_id="other-proj")
    monkeypatch.setenv("MY_BQ_CREDS", json.dumps(other_creds))
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    created = MagicMock()
    created.location = "US"
    created.labels = {}
    mock_client.create_dataset.return_value = created

    resource = mod.BigQueryDdlResource(credentials=CREDS, credentials_env_var="MY_BQ_CREDS", project_id="my-proj")
    resource.ensure_dataset("analytics")

    mock_from_info.assert_called_once_with(CREDS)


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_credentials_path_takes_precedence_over_env_var(mock_client_cls, mock_from_info, mod, monkeypatch, tmp_path):
    import json

    other_creds = dict(CREDS, project_id="other-proj")
    monkeypatch.setenv("MY_BQ_CREDS", json.dumps(other_creds))
    cred_file = tmp_path / "creds.json"
    cred_file.write_text(json.dumps(CREDS))
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    created = MagicMock()
    created.location = "US"
    created.labels = {}
    mock_client.create_dataset.return_value = created

    resource = mod.BigQueryDdlResource(credentials_path=str(cred_file), credentials_env_var="MY_BQ_CREDS", project_id="my-proj")
    resource.ensure_dataset("analytics")

    mock_from_info.assert_called_once_with(CREDS)


# --- table lifecycle -----------------------------------------------------

@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_ensure_table_builds_schema_fields_and_partitioning(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    field_id, field_created = MagicMock(), MagicMock()
    field_id.name = "id"
    field_created.name = "created"
    created = MagicMock()
    created.schema = [field_id, field_created]
    created.time_partitioning = MagicMock(field="created")
    mock_client.create_table.return_value = created

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    schema = [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "created", "type": "DATE"}]
    result = resource.ensure_table("analytics", "orders", schema, table_partition_field="created", cluster_fields=["id"])

    assert result["columns"] == ["id", "created"]
    assert result["table_partition_field"] == "created"
    mock_client.create_table.assert_called_once()
    (table_arg,), kwargs = mock_client.create_table.call_args
    assert kwargs.get("exists_ok") is True
    assert table_arg.clustering_fields == ["id"]
    mock_client.delete_table.assert_not_called()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_recreate_table_deletes_then_creates(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_table.return_value = MagicMock(schema=[], time_partitioning=None)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    resource.recreate_table("analytics", "orders", [{"name": "id", "type": "INTEGER"}])

    mock_client.delete_table.assert_called_once()
    _, del_kwargs = mock_client.delete_table.call_args
    assert del_kwargs["not_found_ok"] is True
    mock_client.create_table.assert_called_once()


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_delete_table_does_not_require_schema(mock_client_cls, _mock_creds, mod):
    from google.api_core.exceptions import NotFound
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_table.side_effect = NotFound("gone")

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.delete_table("analytics", "orders")

    mock_client.delete_table.assert_called_once()
    mock_client.create_table.assert_not_called()
    assert result["exists_after_delete"] is False


# --- value check -----------------------------------------------------

@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_run_value_check_passing(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 42)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.run_value_check("SELECT COUNT(*) FROM t", expected_value=0, comparison="greater_than")

    assert result["passed"] is True
    assert result["actual_value"] == 42


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_run_value_check_failing_does_not_raise_by_itself(mock_client_cls, _mock_creds, mod):
    """The resource reports the outcome; raising on mismatch is the caller's decision."""
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 0)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.run_value_check("SELECT COUNT(*) FROM t", expected_value=0, comparison="greater_than")

    assert result["passed"] is False


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_run_value_check_tolerance_allows_near_match(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 100.02)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.run_value_check("SELECT AVG(x) FROM t", expected_value=100.0, comparison="equal", tolerance=0.1)

    assert result["passed"] is True


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_run_value_check_tolerance_rejects_far_mismatch(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 50.0)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    result = resource.run_value_check("SELECT AVG(x) FROM t", expected_value=100.0, comparison="equal", tolerance=0.1)

    assert result["passed"] is False


@patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock())
@patch("google.cloud.bigquery.Client")
def test_run_value_check_query_params_substitution(mock_client_cls, _mock_creds, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    _mock_query_result(mock_client, 1)

    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    resource.run_value_check(
        "SELECT COUNT(*) FROM `{table}`", expected_value=0, comparison="greater_than",
        query_params={"table": "my-project.analytics.orders"},
    )
    rendered = mock_client.query.call_args[0][0]
    assert rendered == "SELECT COUNT(*) FROM `my-project.analytics.orders`"


def test_run_value_check_unknown_comparison_raises(mod):
    resource = mod.BigQueryDdlResource(credentials=CREDS, project_id="my-proj")
    with patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=MagicMock()), \
         patch("google.cloud.bigquery.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        _mock_query_result(mock_client, 1)
        with pytest.raises(ValueError, match="unknown comparison"):
            resource.run_value_check("SELECT 1", expected_value=1, comparison="bogus")


# --- component registration -----------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.BigQueryDdlResourceComponent(resource_key="my_bq_ddl", project_id="my-proj")
    defs = component.build_defs(context=None)
    assert "my_bq_ddl" in defs.resources
    assert isinstance(defs.resources["my_bq_ddl"], mod.BigQueryDdlResource)
