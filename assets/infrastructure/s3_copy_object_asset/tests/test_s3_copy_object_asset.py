"""Real, committed tests for S3CopyObjectAssetComponent -- mocks only boto3.client."""
import pytest

from .conftest import FakeS3Client, materialize


def _patch(mod, monkeypatch):
    import boto3
    client = FakeS3Client()
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    return client


def test_bucket_key_form_copies_with_default_acl(mod, monkeypatch):
    client = _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
    )
    result = materialize(component)
    assert result.success
    assert client.copy_calls[0] == {
        "Bucket": "dst", "Key": "b.txt", "CopySource": {"Bucket": "src", "Key": "a.txt"}, "ACL": "private",
    }


def test_s3_url_form_for_both_source_and_dest(mod, monkeypatch):
    client = _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_s3_url="s3://src/path/a.txt", dest_s3_url="s3://dst/path/b.txt",
    )
    result = materialize(component)
    assert result.success
    assert client.copy_calls[0]["CopySource"] == {"Bucket": "src", "Key": "path/a.txt"}
    assert client.copy_calls[0]["Bucket"] == "dst" and client.copy_calls[0]["Key"] == "path/b.txt"


def test_custom_acl_passes_through(mod, monkeypatch):
    client = _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
        acl="bucket-owner-full-control",
    )
    materialize(component)
    assert client.copy_calls[0]["ACL"] == "bucket-owner-full-control"


def test_metadata_directive_passes_through_when_set(mod, monkeypatch):
    client = _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
        metadata_directive="REPLACE",
    )
    materialize(component)
    assert client.copy_calls[0]["MetadataDirective"] == "REPLACE"


def test_requires_exactly_one_source_form(mod):
    with pytest.raises(Exception, match="source_s3_url"):
        mod.S3CopyObjectAssetComponent(
            asset_name="t", source_s3_url="s3://a/b", source_bucket="src", source_key="a.txt",
            dest_bucket="dst", dest_key="b.txt",
        )


def test_requires_exactly_one_dest_form(mod):
    with pytest.raises(Exception, match="dest_s3_url"):
        mod.S3CopyObjectAssetComponent(
            asset_name="t", source_bucket="src", source_key="a.txt",
            dest_s3_url="s3://a/b", dest_bucket="dst", dest_key="b.txt",
        )


def test_bare_bucket_without_key_is_rejected(mod):
    with pytest.raises(Exception, match="source_bucket and source_key"):
        mod.S3CopyObjectAssetComponent(
            asset_name="t", source_bucket="src", dest_bucket="dst", dest_key="b.txt",
        )


def test_malformed_s3_url_rejected(mod):
    with pytest.raises(ValueError, match="s3://bucket/key"):
        mod._parse_s3_url("not-an-s3-url")
