"""Real, committed tests for S3DeleteObjectsAssetComponent -- mocks only boto3.client."""
import dagster as dg
import pytest

from .conftest import FakeS3Client


def _materialize(mod, monkeypatch, client, **attrs):
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    component = mod.S3DeleteObjectsAssetComponent(asset_name="t", bucket="my-bucket", **attrs)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_deletes_a_static_key_list(mod, monkeypatch):
    client = FakeS3Client()
    result = _materialize(mod, monkeypatch, client, keys=["a.txt", "b.txt"])
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["deleted_count"].value == 2
    assert sorted(md["deleted_keys"].value) == ["a.txt", "b.txt"]
    assert client.delete_calls[0]["Objects"] == [{"Key": "a.txt"}, {"Key": "b.txt"}]


def test_chunks_at_1000_keys_per_call(mod, monkeypatch):
    client = FakeS3Client()
    keys = [f"k{i}" for i in range(2500)]
    result = _materialize(mod, monkeypatch, client, keys=keys)
    assert result.success
    assert len(client.delete_calls) == 3
    assert [len(c["Objects"]) for c in client.delete_calls] == [1000, 1000, 500]
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["deleted_count"].value == 2500


def test_errors_in_response_fail_the_asset(mod, monkeypatch):
    client = FakeS3Client(errors_on_chunk={0: {"b.txt"}})
    with pytest.raises(Exception):
        _materialize(mod, monkeypatch, client, keys=["a.txt", "b.txt"])


def test_keys_from_upstream_asset(mod, monkeypatch):
    @dg.asset(key=dg.AssetKey.from_user_string("key_source"))
    def key_source():
        return ["x.txt", "y.txt"]

    client = FakeS3Client()
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    component = mod.S3DeleteObjectsAssetComponent(asset_name="t", bucket="my-bucket", keys_asset_key="key_source")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([key_source, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert client.delete_calls[0]["Objects"] == [{"Key": "x.txt"}, {"Key": "y.txt"}]


def test_requires_exactly_one_of_keys_or_keys_asset_key(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.S3DeleteObjectsAssetComponent(asset_name="t", bucket="my-bucket")


def test_rejects_both_keys_and_keys_asset_key(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.S3DeleteObjectsAssetComponent(asset_name="t", bucket="my-bucket", keys=["a"], keys_asset_key="key_source")
