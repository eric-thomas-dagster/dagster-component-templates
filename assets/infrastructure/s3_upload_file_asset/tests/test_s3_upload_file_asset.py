"""Real, committed tests for S3UploadFileAssetComponent -- mocks only boto3.client +
botocore's real ClientError for the 404 existence check (the real external surface)."""
import dagster as dg
import pytest

from .conftest import FakeS3Client


def _materialize(mod, monkeypatch, client, tmp_path, **attrs):
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    local_file = tmp_path / "upload.txt"
    local_file.write_text("hello")
    component = mod.S3UploadFileAssetComponent(asset_name="t", local_path=str(local_file), **attrs)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_uploads_when_destination_does_not_exist(mod, monkeypatch, tmp_path):
    client = FakeS3Client(existing_keys=set())
    result = _materialize(mod, monkeypatch, client, tmp_path, bucket="dst", key="a.txt")
    assert result.success
    assert client.upload_calls[0]["Bucket"] == "dst" and client.upload_calls[0]["Key"] == "a.txt"


def test_replace_false_fails_if_destination_already_exists(mod, monkeypatch, tmp_path):
    client = FakeS3Client(existing_keys={"a.txt"})
    with pytest.raises(Exception):
        _materialize(mod, monkeypatch, client, tmp_path, bucket="dst", key="a.txt")
    assert client.upload_calls == []


def test_replace_true_uploads_even_if_destination_exists(mod, monkeypatch, tmp_path):
    client = FakeS3Client(existing_keys={"a.txt"})
    result = _materialize(mod, monkeypatch, client, tmp_path, bucket="dst", key="a.txt", replace=True)
    assert result.success
    assert len(client.upload_calls) == 1


def test_s3_url_form(mod, monkeypatch, tmp_path):
    client = FakeS3Client()
    result = _materialize(mod, monkeypatch, client, tmp_path, dest_s3_url="s3://dst/path/a.txt")
    assert result.success
    assert client.upload_calls[0]["Bucket"] == "dst" and client.upload_calls[0]["Key"] == "path/a.txt"


def test_extra_args_pass_through(mod, monkeypatch, tmp_path):
    client = FakeS3Client()
    result = _materialize(
        mod, monkeypatch, client, tmp_path, bucket="dst", key="a.txt",
        extra_args={"ACL": "public-read"},
    )
    assert result.success
    assert client.upload_calls[0]["ExtraArgs"] == {"ACL": "public-read"}


def test_requires_exactly_one_destination_form(mod):
    with pytest.raises(Exception, match="dest_s3_url"):
        mod.S3UploadFileAssetComponent(asset_name="t", local_path="x.txt", dest_s3_url="s3://a/b", bucket="dst", key="a.txt")


def test_pool_wires_through(mod, monkeypatch, tmp_path):
    client = FakeS3Client()
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    local_file = tmp_path / "upload.txt"
    local_file.write_text("hello")
    component = mod.S3UploadFileAssetComponent(asset_name="t", local_path=str(local_file), bucket="dst", key="a.txt", pool="s3_bots")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "s3_bots"


def test_retry_policy_wires_through(mod, monkeypatch, tmp_path):
    client = FakeS3Client()
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    local_file = tmp_path / "upload.txt"
    local_file.write_text("hello")
    component = mod.S3UploadFileAssetComponent(
        asset_name="t", local_path=str(local_file), bucket="dst", key="a.txt",
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_upload(mod, monkeypatch, tmp_path):
    import time
    import boto3
    client = FakeS3Client(upload_delay_seconds=1)
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    local_file = tmp_path / "upload.txt"
    local_file.write_text("hello")
    component = mod.S3UploadFileAssetComponent(
        asset_name="t", local_path=str(local_file), bucket="dst", key="a.txt", timeout_seconds=0.05,
    )
    asset_def = component.build_defs(context=None).assets[0]
    start = time.monotonic()
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1
