from unittest.mock import MagicMock, patch


@patch("boto3.client")
def test_create_bucket_us_east_1_omits_location_constraint(mock_boto_client, mod):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    resource = mod.S3BucketResource(region_name="us-east-1")
    resource.create_bucket("my-bucket")

    mock_client.create_bucket.assert_called_once_with(Bucket="my-bucket")


@patch("boto3.client")
def test_create_bucket_other_region_sets_location_constraint(mock_boto_client, mod):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    resource = mod.S3BucketResource(region_name="us-west-2")
    resource.create_bucket("my-bucket")

    mock_client.create_bucket.assert_called_once_with(
        Bucket="my-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )


@patch("boto3.client")
def test_ensure_bucket_creates_when_missing(mock_boto_client, mod):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    resource = mod.S3BucketResource(region_name="us-west-2")
    result = resource.ensure_bucket("my-bucket")

    assert result == {"already_existed": False}


@patch("boto3.client")
def test_ensure_bucket_noop_on_already_owned_by_you(mock_boto_client, mod):
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "CreateBucket"
    )

    resource = mod.S3BucketResource()
    result = resource.ensure_bucket("my-bucket")

    assert result == {"already_existed": True}


@patch("boto3.client")
def test_ensure_bucket_us_east_1_already_exists_quirk(mock_boto_client, mod):
    """us-east-1 legacy behavior: re-creating a bucket you own can surface BucketAlreadyExists
    instead of BucketAlreadyOwnedByYou -- head_bucket confirms it's actually ours."""
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyExists"}}, "CreateBucket"
    )
    mock_client.head_bucket.return_value = {}

    resource = mod.S3BucketResource(region_name="us-east-1")
    result = resource.ensure_bucket("my-bucket")

    assert result == {"already_existed": True}


@patch("boto3.client")
def test_ensure_bucket_other_region_already_exists_but_not_mine_raises(mock_boto_client, mod):
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyExists"}}, "CreateBucket"
    )
    mock_client.head_bucket.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadBucket")

    resource = mod.S3BucketResource()
    try:
        resource.ensure_bucket("my-bucket")
        assert False, "expected ClientError to propagate when the bucket isn't ours"
    except ClientError:
        pass


@patch("boto3.client")
def test_delete_bucket_absent_is_noop(mock_boto_client, mod):
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    resource = mod.S3BucketResource()
    result = resource.delete_bucket("my-bucket")

    assert result == {"existed": False, "objects_deleted": 0}
    mock_client.delete_bucket.assert_not_called()


@patch("boto3.client")
def test_delete_bucket_force_empties_first(mock_boto_client, mod):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.head_bucket.return_value = {}
    paginator = MagicMock()
    mock_client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Versions": [{"Key": "a", "VersionId": "v1"}], "DeleteMarkers": [{"Key": "b", "VersionId": "v2"}]}
    ]

    resource = mod.S3BucketResource()
    result = resource.delete_bucket("my-bucket", force=True)

    assert result == {"existed": True, "objects_deleted": 2}
    mock_client.delete_objects.assert_called_once_with(
        Bucket="my-bucket",
        Delete={"Objects": [{"Key": "a", "VersionId": "v1"}, {"Key": "b", "VersionId": "v2"}]},
    )
    mock_client.delete_bucket.assert_called_once_with(Bucket="my-bucket")


@patch("boto3.client")
def test_delete_bucket_without_force_skips_emptying(mock_boto_client, mod):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.head_bucket.return_value = {}

    resource = mod.S3BucketResource()
    result = resource.delete_bucket("my-bucket", force=False)

    assert result == {"existed": True, "objects_deleted": 0}
    mock_client.get_paginator.assert_not_called()
    mock_client.delete_bucket.assert_called_once_with(Bucket="my-bucket")


def test_component_registers_resource(mod):
    component = mod.S3BucketResourceComponent(resource_key="my_s3_buckets", region_name="us-west-2")
    defs = component.build_defs(context=MagicMock())
    assert "my_s3_buckets" in defs.resources
    assert isinstance(defs.resources["my_s3_buckets"], mod.S3BucketResource)
