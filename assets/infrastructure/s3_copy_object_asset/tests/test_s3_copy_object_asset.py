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


def test_no_pool_by_default(mod, monkeypatch):
    _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool is None


def test_pool_wires_through(mod, monkeypatch):
    _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
        pool="s3_bots",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "s3_bots"


def test_retry_policy_wires_through(mod, monkeypatch):
    _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_unset_does_not_affect_a_normal_copy(mod, monkeypatch):
    _patch(mod, monkeypatch)
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
    )
    result = materialize(component)
    assert result.success


def test_timeout_seconds_fails_a_slow_copy(mod, monkeypatch):
    import time
    client = _patch(mod, monkeypatch)
    client.copy_delay_seconds = 1
    component = mod.S3CopyObjectAssetComponent(
        asset_name="t", source_bucket="src", source_key="a.txt", dest_bucket="dst", dest_key="b.txt",
        timeout_seconds=0.05,
    )
    start = time.monotonic()
    result = materialize(component, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1  # failed on the timeout, not by waiting out the full delay
