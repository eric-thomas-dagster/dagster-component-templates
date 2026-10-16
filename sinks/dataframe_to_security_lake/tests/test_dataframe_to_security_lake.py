"""Committed tests for DataframeToSecurityLakeComponent's resource_key retrofit.

The S3 client is the only faked piece. boto3.Session(...) is real (it makes no
network call until a client is used), so the inline path is exercised by
replacing only `Session.client` with the fake S3 client.
"""
import io

import boto3
import dagster as dg
import pandas as pd
import pyarrow.parquet as pq
import pytest

from .conftest import FakeS3Client, FakeS3Resource, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _events():
    # OCSF `time` is epoch ms; two distinct UTC days -> two eventDay partitions.
    return pd.DataFrame({
        "time": [1704153600000, 1704153660000, 1704240000000],  # 2024-01-02 x2, 2024-01-03
        "activity_id": [1, 2, 3],
    })


@dg.asset(name="ocsf_events")
def _upstream() -> pd.DataFrame:
    return _events()


def _component(mod, **kw):
    return mod.DataframeToSecurityLakeComponent(
        asset_name="to_lake", upstream_asset_key="ocsf_events", bucket="aws-security-data-lake-test",
        source_location="ext-dagster", account_id="123456789012", **kw)


def _read_uploads(client):
    return sorted(
        (key, pq.read_table(io.BytesIO(data)).to_pandas()["activity_id"].tolist())
        for _, key, data in client.uploads
    )


def test_resource_key_defaults_to_none_and_uses_boto3_session(mod, monkeypatch):
    comp = _component(mod)
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    inline = FakeS3Client("inline")
    monkeypatch.setattr(boto3.Session, "client", lambda self, name, **kw: inline)
    assert dg.materialize([_upstream, sink]).success
    got = _read_uploads(inline)
    assert [(k.split("/")[3], ids) for k, ids in got] == [("eventDay=20240102", [1, 2]), ("eventDay=20240103", [3])]
    assert all(b == "aws-security-data-lake-test" for b, _, _ in inline.uploads)


def test_resource_key_routes_s3_uploads_through_registered_resource(mod, monkeypatch):
    def _boom(self, name, **kw):
        raise AssertionError("inline boto3 session client must not be used when resource_key is set")

    monkeypatch.setattr(boto3.Session, "client", _boom)
    shared = FakeS3Client("resource")
    comp = _component(mod, resource_key="s3_shared")
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"s3_shared"}
    r = dg.materialize([_upstream, sink], resources={"s3_shared": FakeS3Resource(shared)})
    assert r.success
    got = _read_uploads(shared)
    assert [(k.split("/")[3], ids) for k, ids in got] == [("eventDay=20240102", [1, 2]), ("eventDay=20240103", [3])]
    md = r.asset_materializations_for_node("to_lake")[0].metadata
    assert md["rows_written"].value == 3 and md["files_written"].value == 2


def test_resource_key_not_registered_fails_loudly(mod):
    sink = _component(mod, resource_key="nope").build_defs(None).assets[0]
    with pytest.raises(Exception, match="nope"):
        dg.materialize([_upstream, sink])
