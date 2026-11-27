from unittest.mock import MagicMock, patch


@patch("google.cloud.storage.Client")
def test_ensure_bucket_creates_when_missing(mock_client_cls, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    bucket = MagicMock()
    mock_client.bucket.return_value = bucket

    resource = mod.GcsBucketResource()
    result = resource.ensure_bucket("my-bucket", location="US", storage_class="STANDARD")

    assert result == {"already_existed": False}
    mock_client.create_bucket.assert_called_once_with(bucket, location="US")
    assert bucket.storage_class == "STANDARD"


@patch("google.cloud.storage.Client")
def test_ensure_bucket_is_noop_when_already_owned(mock_client_cls, mod):
    from google.api_core.exceptions import Conflict

    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_bucket.side_effect = Conflict("already exists")
    mock_client.get_bucket.return_value = MagicMock()

    resource = mod.GcsBucketResource()
    result = resource.ensure_bucket("my-bucket")

    assert result == {"already_existed": True}
    mock_client.get_bucket.assert_called_once_with("my-bucket")


@patch("google.cloud.storage.Client")
def test_create_bucket_raises_on_conflict(mock_client_cls, mod):
    from google.api_core.exceptions import Conflict

    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_bucket.side_effect = Conflict("already exists")

    resource = mod.GcsBucketResource()
    try:
        resource.create_bucket("my-bucket")
        assert False, "expected Conflict to propagate"
    except Conflict:
        pass


@patch("google.cloud.storage.Client")
def test_delete_bucket_absent_is_noop(mock_client_cls, mod):
    from google.api_core.exceptions import NotFound

    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.get_bucket.side_effect = NotFound("gone")

    resource = mod.GcsBucketResource()
    result = resource.delete_bucket("my-bucket")

    assert result == {"existed": False, "objects_deleted": 0}


@patch("google.cloud.storage.Client")
def test_delete_bucket_force_empties_first(mock_client_cls, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    bucket = MagicMock()
    mock_client.get_bucket.return_value = bucket
    blob1, blob2 = MagicMock(), MagicMock()
    mock_client.list_blobs.return_value = [blob1, blob2]

    resource = mod.GcsBucketResource()
    result = resource.delete_bucket("my-bucket", force=True)

    assert result == {"existed": True, "objects_deleted": 2}
    blob1.delete.assert_called_once()
    blob2.delete.assert_called_once()
    bucket.delete.assert_called_once_with(force=False)


@patch("google.cloud.storage.Client")
def test_delete_bucket_without_force_does_not_list_blobs(mock_client_cls, mod):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    bucket = MagicMock()
    mock_client.get_bucket.return_value = bucket

    resource = mod.GcsBucketResource()
    result = resource.delete_bucket("my-bucket", force=False)

    assert result == {"existed": True, "objects_deleted": 0}
    mock_client.list_blobs.assert_not_called()


def test_credentials_env_var_required_message(mod):
    resource = mod.GcsBucketResource(credentials_env_var="NONEXISTENT_ENV_VAR_XYZ")
    try:
        resource._client()
        assert False, "expected KeyError for unset env var"
    except KeyError as e:
        assert "NONEXISTENT_ENV_VAR_XYZ" in str(e)


def test_component_registers_resource(mod):
    import dagster as dg

    component = mod.GcsBucketResourceComponent(resource_key="my_gcs_buckets", credentials_env_var=None)
    defs = component.build_defs(context=MagicMock())
    assert "my_gcs_buckets" in defs.resources
    assert isinstance(defs.resources["my_gcs_buckets"], mod.GcsBucketResource)
