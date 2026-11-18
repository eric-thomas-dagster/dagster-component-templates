"""Real, committed tests for S3CleanupJobComponent -- real job execute_in_process(), mocking
only boto3 (the real external call)."""
import datetime as dt

import pytest

from .conftest import FakeS3Client


NOW = dt.datetime.now(dt.timezone.utc)
OLD = NOW - dt.timedelta(days=30)
RECENT = NOW - dt.timedelta(days=1)


def _run(mod, monkeypatch, fake_client, **attrs):
    import boto3
    monkeypatch.setattr(boto3, "Session", lambda **k: type("S", (), {"client": lambda self, name: fake_client})())
    component = mod.S3CleanupJobComponent(job_name="cleanup", bucket="my-bucket", **attrs)
    job = component.build_defs(context=None).jobs[0]
    return job.execute_in_process()


def test_older_than_days_deletes_only_stale_objects(mod, monkeypatch):
    client = FakeS3Client([
        {"Key": "a/old.txt", "LastModified": OLD},
        {"Key": "a/recent.txt", "LastModified": RECENT},
    ])
    result = _run(mod, monkeypatch, client, older_than_days=7)
    assert result.success
    assert client.delete_calls == [[{"Key": "a/old.txt"}]]


def test_dry_run_scans_but_does_not_delete(mod, monkeypatch):
    client = FakeS3Client([{"Key": "a/old.txt", "LastModified": OLD}])
    result = _run(mod, monkeypatch, client, older_than_days=7, dry_run=True)
    assert result.success
    assert client.delete_calls == []


def test_static_keys_field_deletes_explicit_keys_regardless_of_age(mod, monkeypatch):
    client = FakeS3Client([
        {"Key": "a/keep.txt", "LastModified": RECENT},
        {"Key": "a/explicit.txt", "LastModified": RECENT},
    ])
    result = _run(mod, monkeypatch, client, keys=["a/explicit.txt"])
    assert result.success
    assert client.delete_calls == [[{"Key": "a/explicit.txt"}]]


def test_run_config_keys_combine_with_static_keys(mod, monkeypatch):
    client = FakeS3Client([])
    component = mod.S3CleanupJobComponent(job_name="cleanup", bucket="my-bucket", keys=["static.txt"])
    import boto3
    monkeypatch.setattr(boto3, "Session", lambda **k: type("S", (), {"client": lambda self, name: client})())
    job = component.build_defs(context=None).jobs[0]
    result = job.execute_in_process(
        run_config={"ops": {"cleanup_op": {"config": {"keys": ["from_run_config.txt"]}}}},
    )
    assert result.success
    deleted_keys = {k["Key"] for batch in client.delete_calls for k in batch}
    assert deleted_keys == {"static.txt", "from_run_config.txt"}


def test_older_than_days_and_keys_both_apply_together(mod, monkeypatch):
    client = FakeS3Client([
        {"Key": "a/old.txt", "LastModified": OLD},
        {"Key": "a/recent.txt", "LastModified": RECENT},
    ])
    result = _run(mod, monkeypatch, client, older_than_days=7, keys=["a/recent.txt"])
    assert result.success
    deleted_keys = {k["Key"] for batch in client.delete_calls for k in batch}
    assert deleted_keys == {"a/old.txt", "a/recent.txt"}


def test_neither_older_than_days_nor_keys_deletes_nothing(mod, monkeypatch):
    client = FakeS3Client([{"Key": "a/x.txt", "LastModified": OLD}])
    result = _run(mod, monkeypatch, client)
    assert result.success
    assert client.delete_calls == []
