"""S3MonitorSensorComponent: dynamic_partition_keep_days prunes partitions this sensor
registered once they're older than keep_days, via a local JSON state file independent of the
sensor's own cursor. Mocks boto3 (the only real external surface) with an empty bucket listing
so these tests exercise the pruning path in isolation from object discovery."""
import json
from datetime import datetime, timedelta, timezone

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


class _FakePaginator:
    def paginate(self, Bucket, Prefix):
        return [{"Contents": []}]  # empty bucket -- no new objects this tick


class _FakeS3Client:
    def get_paginator(self, name):
        return _FakePaginator()


def _sensor(mod, state_path, monkeypatch, **attrs):
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeS3Client())
    component = mod.S3MonitorSensorComponent(
        sensor_name="watch", bucket_name="my-bucket", prefix="inbox/", job_name="extract_job",
        partition_mode="dynamic_partition", dynamic_partitions_name="files",
        dynamic_partition_state_path=state_path, **attrs,
    )
    return list(component.build_defs(None).sensors)[0]


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


def _delete_requests(result):
    return [r for r in (result.dynamic_partitions_requests or []) if type(r).__name__ == "DeleteDynamicPartitionsRequest"]


def test_keep_days_unset_never_touches_the_state_file(mod, tmp_path, instance, monkeypatch):
    state_path = str(tmp_path / "state.json")
    _tick(_sensor(mod, state_path, monkeypatch), instance)
    import os
    assert not os.path.exists(state_path)


def test_partitions_older_than_keep_days_are_pruned(mod, tmp_path, instance, monkeypatch):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    result = _tick(_sensor(mod, state_path, monkeypatch, dynamic_partition_keep_days=7), instance)

    delete_reqs = _delete_requests(result)
    assert len(delete_reqs) == 1
    assert delete_reqs[0].partition_keys == ["stale_key"]
    remaining = json.loads(open(state_path).read())
    assert "stale_key" not in remaining


def test_partitions_within_keep_days_are_not_pruned(mod, tmp_path, instance, monkeypatch):
    state_path = str(tmp_path / "state.json")
    recent_seen = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"fresh_key": recent_seen}, f)

    result = _tick(_sensor(mod, state_path, monkeypatch, dynamic_partition_keep_days=7), instance)

    assert _delete_requests(result) == []
    remaining = json.loads(open(state_path).read())
    assert "fresh_key" in remaining


def test_pruning_runs_even_on_a_quiet_tick_with_no_new_objects(mod, tmp_path, instance, monkeypatch):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    result = _tick(_sensor(mod, state_path, monkeypatch, dynamic_partition_keep_days=7), instance)

    assert not result.run_requests
    assert len(_delete_requests(result)) == 1
