"""Real, committed tests for S3CreateObjectAssetComponent -- mocks only boto3.client +
botocore's real ClientError for the 404 existence check (the real external surface)."""
import time

import dagster as dg
import pytest

from .conftest import FakeS3Client, materialize


def _patch(mod, monkeypatch, client):
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    return client


def test_writes_literal_data_when_destination_does_not_exist(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="hello world", bucket="dst", key="a.txt")
    result = materialize(component)
    assert result.success
    assert client.put_calls[0]["Bucket"] == "dst" and client.put_calls[0]["Key"] == "a.txt"
    assert client.put_calls[0]["Body"] == b"hello world"
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["bytes_written"].value == 11


def test_replace_false_fails_if_destination_already_exists(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client(existing_keys={"a.txt"}))
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="x", bucket="dst", key="a.txt")
    result = materialize(component, raise_on_error=False)
    assert not result.success
    assert client.put_calls == []


def test_replace_true_writes_even_if_destination_exists(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client(existing_keys={"a.txt"}))
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="x", bucket="dst", key="a.txt", replace=True)
    result = materialize(component)
    assert result.success
    assert len(client.put_calls) == 1


def test_s3_url_form(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="x", dest_s3_url="s3://dst/path/a.txt")
    result = materialize(component)
    assert result.success
    assert client.put_calls[0]["Bucket"] == "dst" and client.put_calls[0]["Key"] == "path/a.txt"


def test_content_type_passes_through(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="{}", bucket="dst", key="a.json", content_type="application/json")
    materialize(component)
    assert client.put_calls[0]["ContentType"] == "application/json"


def test_data_from_upstream_asset(mod, monkeypatch):
    @dg.asset(key=dg.AssetKey.from_user_string("data_source"))
    def data_source():
        return "from-upstream"

    client = _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data_asset_key="data_source", bucket="dst", key="a.txt")
    result = materialize(component, assets_extra=[data_source])
    assert result.success
    assert client.put_calls[0]["Body"] == b"from-upstream"


def test_requires_exactly_one_of_data_or_data_asset_key(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.S3CreateObjectAssetComponent(asset_name="t", bucket="dst", key="a.txt")


def test_requires_exactly_one_destination_form(mod):
    with pytest.raises(Exception, match="dest_s3_url"):
        mod.S3CreateObjectAssetComponent(asset_name="t", data="x", dest_s3_url="s3://a/b", bucket="dst", key="a.txt")


def test_pool_wires_through(mod, monkeypatch):
    _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="x", bucket="dst", key="a.txt", pool="s3_bots")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "s3_bots"


def test_retry_policy_wires_through(mod, monkeypatch):
    _patch(mod, monkeypatch, FakeS3Client())
    component = mod.S3CreateObjectAssetComponent(
        asset_name="t", data="x", bucket="dst", key="a.txt",
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_write(mod, monkeypatch):
    client = _patch(mod, monkeypatch, FakeS3Client(put_delay_seconds=1))
    component = mod.S3CreateObjectAssetComponent(asset_name="t", data="x", bucket="dst", key="a.txt", timeout_seconds=0.05)
    start = time.monotonic()
    result = materialize(component, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1
