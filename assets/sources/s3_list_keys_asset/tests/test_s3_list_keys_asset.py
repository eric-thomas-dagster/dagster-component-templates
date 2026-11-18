"""Real, committed tests for S3ListKeysAssetComponent -- mocks only boto3.client (the real
external call)."""
from .conftest import FakeS3Client, materialize


def _component(mod, monkeypatch, pages, **attrs):
    import boto3
    client = FakeS3Client(pages)
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    component = mod.S3ListKeysAssetComponent(asset_name="t", bucket="my-bucket", **attrs)
    return component, client


def test_lists_keys_across_pages(mod, monkeypatch):
    pages = [
        {"Contents": [{"Key": "a/1.txt"}, {"Key": "a/2.txt"}]},
        {"Contents": [{"Key": "a/3.txt"}]},
    ]
    component, client = _component(mod, monkeypatch, pages, prefix="a/")
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == ["a/1.txt", "a/2.txt", "a/3.txt"]
    assert md["count"].value == 3
    assert client.paginator.last_kwargs == {"Bucket": "my-bucket", "Prefix": "a/"}


def test_delimiter_surfaces_common_prefixes_separately(mod, monkeypatch):
    pages = [{
        "Contents": [{"Key": "a/file.txt"}],
        "CommonPrefixes": [{"Prefix": "a/sub1/"}, {"Prefix": "a/sub2/"}],
    }]
    component, client = _component(mod, monkeypatch, pages, prefix="a/", delimiter="/")
    result = materialize(component)
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == ["a/file.txt"]
    assert md["common_prefixes"].value == ["a/sub1/", "a/sub2/"]
    assert client.paginator.last_kwargs == {"Bucket": "my-bucket", "Prefix": "a/", "Delimiter": "/"}


def test_no_delimiter_omits_delimiter_kwarg(mod, monkeypatch):
    component, client = _component(mod, monkeypatch, [{"Contents": []}])
    materialize(component)
    assert "Delimiter" not in client.paginator.last_kwargs


def test_empty_bucket_returns_empty_lists(mod, monkeypatch):
    component, client = _component(mod, monkeypatch, [{"Contents": []}])
    result = materialize(component)
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == []
    assert md["count"].value == 0


def test_return_keys_as_value_off_by_default_still_succeeds(mod, monkeypatch):
    component, _ = _component(mod, monkeypatch, [{"Contents": [{"Key": "a"}]}])
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == ["a"]


def test_return_keys_as_value_makes_keys_the_materialized_value(mod, monkeypatch):
    pages = [{"Contents": [{"Key": "a/1.txt"}, {"Key": "a/2.txt"}]}]
    component, _ = _component(mod, monkeypatch, pages, prefix="a/", return_keys_as_value=True)
    result = materialize(component)
    assert result.success
    assert result.output_for_node("t") == ["a/1.txt", "a/2.txt"]
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == ["a/1.txt", "a/2.txt"]


def test_resource_key_uses_the_registered_resource(mod, monkeypatch):
    pages = [{"Contents": [{"Key": "x"}]}]
    client = FakeS3Client(pages)

    class FakeResource:
        def get_client(self):
            return client

    component = mod.S3ListKeysAssetComponent(asset_name="t", bucket="my-bucket", resource_key="s3res")
    asset_def = component.build_defs(context=None).assets[0]
    import dagster as dg
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager(), "s3res": FakeResource()})
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["keys"].value == ["x"]
