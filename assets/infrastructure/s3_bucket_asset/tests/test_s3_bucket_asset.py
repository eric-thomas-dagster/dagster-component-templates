"""Real, committed tests for S3BucketAssetComponent.

Mocks only the external call (a fake boto3 S3 client matching the real botocore ClientError
shape) -- everything else (idempotency logic, the us-east-1 LocationConstraint special case,
force-delete object enumeration, Dagster asset wiring) is real.
"""
import dagster as dg
import pytest
from botocore.exceptions import ClientError

from .conftest import load_component_module

mod = load_component_module()


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": "boom"}}, "CreateBucket")


class FakeS3Client:
    def __init__(self, *, create_raises=None, head_raises_not_found=False):
        self.calls = []
        self._create_raises = create_raises
        self._head_raises_not_found = head_raises_not_found
        self._versions = []

    def create_bucket(self, **kwargs):
        self.calls.append(("create_bucket", kwargs))
        if self._create_raises:
            raise self._create_raises
        return {}

    def head_bucket(self, **kwargs):
        self.calls.append(("head_bucket", kwargs))
        if self._head_raises_not_found:
            raise _client_error("404")
        return {}

    def delete_bucket(self, **kwargs):
        self.calls.append(("delete_bucket", kwargs))

    def get_paginator(self, name):
        assert name == "list_object_versions"
        client = self

        class _Paginator:
            def paginate(self, **kwargs):
                return client._versions

        return _Paginator()

    def delete_objects(self, **kwargs):
        self.calls.append(("delete_objects", kwargs))


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_s3_client", lambda region_name: client)


def _comp(**kw):
    args = dict(asset_name="out", bucket_name="my-bucket")
    args.update(kw)
    return mod.S3BucketAssetComponent(**args)


def test_ensure_exists_creates_when_missing(monkeypatch):
    client = FakeS3Client()
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["already_existed"].value is False
    assert client.calls == [("create_bucket", {"Bucket": "my-bucket"})]


def test_ensure_exists_is_noop_when_bucket_already_owned_by_you(monkeypatch):
    client = FakeS3Client(create_raises=_client_error("BucketAlreadyOwnedByYou"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["already_existed"].value is True


def test_ensure_exists_us_east_1_already_exists_confirmed_via_head_bucket(monkeypatch):
    client = FakeS3Client(create_raises=_client_error("BucketAlreadyExists"), head_raises_not_found=False)
    _patch_client(monkeypatch, client)
    comp = _comp(action="ensure_exists", aws_region="us-east-1")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["already_existed"].value is True


def test_create_action_does_not_swallow_already_owned_by_you(monkeypatch):
    client = FakeS3Client(create_raises=_client_error("BucketAlreadyOwnedByYou"))
    _patch_client(monkeypatch, client)
    comp = _comp(action="create")
    with pytest.raises(ClientError):
        dg.materialize(list(comp.build_defs(None).assets))


def test_us_east_1_region_omits_location_constraint(monkeypatch):
    client = FakeS3Client()
    _patch_client(monkeypatch, client)
    comp = _comp(aws_region="us-east-1")
    dg.materialize(list(comp.build_defs(None).assets))
    assert client.calls == [("create_bucket", {"Bucket": "my-bucket"})]


def test_other_region_sets_location_constraint(monkeypatch):
    client = FakeS3Client()
    _patch_client(monkeypatch, client)
    comp = _comp(aws_region="us-west-2")
    dg.materialize(list(comp.build_defs(None).assets))
    assert client.calls == [
        ("create_bucket", {"Bucket": "my-bucket", "CreateBucketConfiguration": {"LocationConstraint": "us-west-2"}})
    ]


def test_delete_is_noop_when_bucket_already_absent(monkeypatch):
    client = FakeS3Client(head_raises_not_found=True)
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["existed_before"].value is False
    assert ("delete_bucket", {"Bucket": "my-bucket"}) not in client.calls


def test_delete_without_force_does_not_empty_bucket(monkeypatch):
    client = FakeS3Client(head_raises_not_found=False)
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["existed_before"].value is True
    assert md["objects_deleted"].value == 0
    assert ("delete_objects", {"Bucket": "my-bucket", "Delete": {"Objects": []}}) not in client.calls
    assert ("delete_bucket", {"Bucket": "my-bucket"}) in client.calls


def test_delete_with_force_empties_bucket_first(monkeypatch):
    client = FakeS3Client(head_raises_not_found=False)
    client._versions = [
        {"Versions": [{"Key": "a.txt", "VersionId": "v1"}], "DeleteMarkers": [{"Key": "b.txt", "VersionId": "v2"}]},
    ]
    _patch_client(monkeypatch, client)
    comp = _comp(action="delete", force_delete=True)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    md = result.asset_materializations_for_node("out")[0].metadata
    assert md["objects_deleted"].value == 2
    delete_calls = [c for c in client.calls if c[0] == "delete_objects"]
    assert len(delete_calls) == 1
    assert delete_calls[0][1]["Delete"]["Objects"] == [
        {"Key": "a.txt", "VersionId": "v1"},
        {"Key": "b.txt", "VersionId": "v2"},
    ]
    # delete_bucket must happen after delete_objects
    assert [c[0] for c in client.calls].index("delete_objects") < [c[0] for c in client.calls].index("delete_bucket")
