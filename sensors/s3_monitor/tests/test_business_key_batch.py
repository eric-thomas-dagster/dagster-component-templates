"""S3MonitorSensorComponent: partition_mode='business_key_batch' groups this tick's new
objects by a business-key value extracted from each key, and yields ONE RunRequest per
key-with-new-objects-this-tick (never one per file), targeting a MultiPartitionKey of
(business_key_dimension_name, batch_dimension_name). Mocks boto3 (the only real external
surface) with a fake bucket listing so these tests exercise the grouping/validation logic
in isolation from real S3."""
from datetime import datetime, timezone

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


def _obj(key, *, modified=None, size=10, etag="etag"):
    return {
        "Key": key,
        "LastModified": modified or datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc),
        "Size": size,
        "ETag": f'"{etag}"',
    }


class _FakePaginator:
    def __init__(self, contents):
        self._contents = contents

    def paginate(self, Bucket, Prefix):
        return [{"Contents": self._contents}]


class _FakeS3Client:
    def __init__(self, contents):
        self._contents = contents

    def get_paginator(self, name):
        return _FakePaginator(self._contents)


def _sensor(mod, monkeypatch, contents, **attrs):
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeS3Client(contents))
    component = mod.S3MonitorSensorComponent(
        sensor_name="bank_watch",
        bucket_name="my-bucket",
        prefix="incoming/",
        job_name="bank_ingestion_job",
        partition_mode="business_key_batch",
        business_key_dimension_name="bank",
        business_keys=["bank_alpha", "bank_beta"],
        batch_dimension_name="batch_day",
        op_name="bank_batch_staged",
        **attrs,
    )
    return list(component.build_defs(None).sensors)[0]


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


def test_one_run_request_per_business_key_not_per_file(mod, instance, monkeypatch):
    contents = [
        _obj("incoming/bank_alpha/f1.enc", etag="e1"),
        _obj("incoming/bank_alpha/f2.enc", etag="e2"),
        _obj("incoming/bank_beta/f3.enc", etag="e3"),
    ]
    result = _tick(_sensor(mod, monkeypatch, contents), instance)

    assert len(result.run_requests) == 2
    by_bank = {rr.partition_key.keys_by_dimension["bank"]: rr for rr in result.run_requests}
    assert set(by_bank) == {"bank_alpha", "bank_beta"}


def test_run_config_lists_exactly_this_ticks_files_for_that_key(mod, instance, monkeypatch):
    contents = [
        _obj("incoming/bank_alpha/f1.enc", etag="e1", size=111),
        _obj("incoming/bank_alpha/f2.enc", etag="e2", size=222),
    ]
    result = _tick(_sensor(mod, monkeypatch, contents), instance)

    assert len(result.run_requests) == 1
    rr = result.run_requests[0]
    files = rr.run_config["ops"]["bank_batch_staged"]["config"]["files"]
    assert len(files) == 2
    assert {f["key"] for f in files} == {"incoming/bank_alpha/f1.enc", "incoming/bank_alpha/f2.enc"}
    assert {f["size"] for f in files} == {111, 222}


def test_partition_key_targets_batch_day_dimension(mod, instance, monkeypatch):
    contents = [_obj("incoming/bank_alpha/f1.enc")]
    result = _tick(_sensor(mod, monkeypatch, contents), instance)

    pk = result.run_requests[0].partition_key
    assert isinstance(pk, dg.MultiPartitionKey)
    assert pk.keys_by_dimension["bank"] == "bank_alpha"
    assert pk.keys_by_dimension["batch_day"] == datetime.now(timezone.utc).date().isoformat()


def test_object_with_unrecognized_business_key_is_skipped_not_failed(mod, instance, monkeypatch):
    contents = [
        _obj("incoming/bank_alpha/f1.enc", etag="e1"),
        _obj("incoming/bank_unknown/f2.enc", etag="e2"),
    ]
    result = _tick(_sensor(mod, monkeypatch, contents), instance)

    # The unknown-key object doesn't crash the tick and doesn't block bank_alpha's run.
    assert len(result.run_requests) == 1
    assert result.run_requests[0].partition_key.keys_by_dimension["bank"] == "bank_alpha"


def test_custom_business_key_template(mod, instance, monkeypatch):
    import boto3
    contents = [_obj("incoming/raw/bank_alpha_20261125.enc")]
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeS3Client(contents))
    component = mod.S3MonitorSensorComponent(
        sensor_name="bank_watch", bucket_name="my-bucket", prefix="incoming/",
        job_name="bank_ingestion_job", partition_mode="business_key_batch",
        business_key_dimension_name="bank", batch_dimension_name="batch_day",
        op_name="bank_batch_staged",
        business_key_template="{file_stem}",
        business_keys=["bank_alpha_20261125"],
    )
    sensor = list(component.build_defs(None).sensors)[0]
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1
    assert result.run_requests[0].partition_key.keys_by_dimension["bank"] == "bank_alpha_20261125"


def test_no_new_objects_yields_skip_reason(mod, instance, monkeypatch):
    result = _tick(_sensor(mod, monkeypatch, []), instance)
    assert not result.run_requests
    assert result.skip_reason


def test_requires_nonempty_business_keys(mod):
    component = mod.S3MonitorSensorComponent(
        sensor_name="bank_watch", bucket_name="my-bucket", prefix="incoming/",
        job_name="bank_ingestion_job", partition_mode="business_key_batch",
        business_keys=None,
    )
    with pytest.raises(ValueError, match="business_keys"):
        component.build_defs(None)


def test_rejects_dynamic_partitions_name_combo(mod):
    component = mod.S3MonitorSensorComponent(
        sensor_name="bank_watch", bucket_name="my-bucket", prefix="incoming/",
        job_name="bank_ingestion_job", partition_mode="business_key_batch",
        business_keys=["bank_alpha"], dynamic_partitions_name="files",
    )
    with pytest.raises(ValueError, match="dynamic_partitions_name"):
        component.build_defs(None)


def test_rejects_non_daily_batch_window(mod):
    component = mod.S3MonitorSensorComponent(
        sensor_name="bank_watch", bucket_name="my-bucket", prefix="incoming/",
        job_name="bank_ingestion_job", partition_mode="business_key_batch",
        business_keys=["bank_alpha"], batch_window="hourly",
    )
    with pytest.raises(ValueError, match="daily"):
        component.build_defs(None)
