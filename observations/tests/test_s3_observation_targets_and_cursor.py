"""s3_observation_sensor: `targets:` (N feeds per instance) + real cursor-carried
skip-when-unchanged -- component-feedback/2026-11-23-aws-batch-pipes.md item 4.

test_s3_observation.py's own `run_sensor` helper builds a FRESH
build_sensor_context (cursor=None) every call, so it never exercises a real
cursor hand-off between ticks -- these tests pass an explicit cursor string
from tick 1's result into tick 2's context, the way Dagster's sensor daemon
actually does in production.
"""
import json

import dagster as dg
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from .conftest import load_s3

s3 = load_s3()
S3ObservationSensorComponent = s3.S3ObservationSensorComponent


class FakeS3Observer:
    def __init__(self, data_version: str, **extra):
        self._dv = data_version
        self._extra = extra
        self.calls = []

    def observe(self, source: str):
        self.calls.append(source)
        return {"data_version": self._dv, **self._extra}


def _run(defs, resources, cursor=None):
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context(resources=resources, cursor=cursor)
    return sensor_def(ctx)


# ── Mutual exclusivity ───────────────────────────────────────────────────────

def test_single_and_targets_mutually_exclusive():
    import pytest
    with pytest.raises(Exception, match="not both"):
        S3ObservationSensorComponent(
            sensor_name="s3", asset_key="a", bucket_name="b",
            targets=[{"asset_key": "x", "bucket_name": "y"}],
        )


def test_requires_one_mode():
    import pytest
    with pytest.raises(Exception, match="Set either"):
        S3ObservationSensorComponent(sensor_name="s3")


# ── targets: N feeds, one sensor, one tick ──────────────────────────────────

def test_targets_declares_multiple_feeds_one_sensor():
    comp = S3ObservationSensorComponent(
        sensor_name="s3_multi",
        targets=[
            {"asset_key": "raw/orders", "bucket_name": "bucket-a", "prefix": "orders/"},
            {"asset_key": "raw/customers", "bucket_name": "bucket-b"},
        ],
        resource_key="s3_obs",
    )
    defs = comp.build_defs(None)
    assert len(list(defs.sensors)) == 1


def test_targets_each_polled_with_own_source(monkeypatch):
    observer = FakeS3Observer("v1")
    comp = S3ObservationSensorComponent(
        sensor_name="s3_multi",
        targets=[
            {"asset_key": "raw/orders", "bucket_name": "bucket-a", "prefix": "orders/"},
            {"asset_key": "raw/customers", "bucket_name": "bucket-b"},
        ],
        resource_key="s3_obs",
    )
    defs = comp.build_defs(None)
    _run(defs, {"s3_obs": observer})
    assert observer.calls == ["bucket-a/orders/", "bucket-b"]


def test_targets_emit_under_each_resolved_asset_key(monkeypatch):
    observer = FakeS3Observer("v1")
    comp = S3ObservationSensorComponent(
        sensor_name="s3_multi",
        targets=[
            {"asset_key": "raw/orders", "bucket_name": "bucket-a"},
            {"asset_key": "raw/customers", "bucket_name": "bucket-b"},
        ],
        resource_key="s3_obs",
    )
    defs = comp.build_defs(None)
    result = _run(defs, {"s3_obs": observer})
    keys = {e.asset_key for e in result.asset_events}
    assert keys == {dg.AssetKey.from_user_string("raw/orders"), dg.AssetKey.from_user_string("raw/customers")}


# ── Real cursor hand-off: skip-when-unchanged (the core feedback fix) ──────

def test_unchanged_data_version_emits_nothing_on_next_tick():
    comp = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="raw/orders", bucket_name="bucket-a", resource_key="s3_obs",
    )
    defs = comp.build_defs(None)

    tick1 = _run(defs, {"s3_obs": FakeS3Observer("5-2024-01-01T00:00:00")})
    assert len(tick1.asset_events) == 1

    tick2 = _run(defs, {"s3_obs": FakeS3Observer("5-2024-01-01T00:00:00")}, cursor=tick1.cursor)
    assert tick2.asset_events == []
    assert tick2.skip_reason.skip_message == "no target changed since last tick"


def test_changed_data_version_still_emits_on_next_tick():
    comp = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="raw/orders", bucket_name="bucket-a", resource_key="s3_obs",
    )
    defs = comp.build_defs(None)

    tick1 = _run(defs, {"s3_obs": FakeS3Observer("5-t1")})
    tick2 = _run(defs, {"s3_obs": FakeS3Observer("6-t2")}, cursor=tick1.cursor)
    assert len(tick2.asset_events) == 1
    assert tick2.asset_events[0].tags[DATA_VERSION_TAG] == "6-t2"


def test_targets_each_tracked_independently_in_cursor():
    """One target changing must not suppress -- or force -- emission for the other."""
    comp = S3ObservationSensorComponent(
        sensor_name="s3_multi",
        targets=[
            {"asset_key": "raw/orders", "bucket_name": "bucket-a"},
            {"asset_key": "raw/customers", "bucket_name": "bucket-b"},
        ],
        resource_key="s3_obs",
    )
    defs = comp.build_defs(None)

    class _MultiObserver:
        def __init__(self, versions):
            self._versions = versions

        def observe(self, source):
            return {"data_version": self._versions[source]}

    tick1 = _run(defs, {"s3_obs": _MultiObserver({"bucket-a": "v1", "bucket-b": "v1"})})
    assert len(tick1.asset_events) == 2

    tick2 = _run(defs, {"s3_obs": _MultiObserver({"bucket-a": "v1", "bucket-b": "v2"})}, cursor=tick1.cursor)
    assert len(tick2.asset_events) == 1
    assert tick2.asset_events[0].asset_key == dg.AssetKey.from_user_string("raw/customers")


def test_native_boto3_path_also_skips_when_unchanged(monkeypatch):
    """The skip-on-unchanged fix applies to the native boto3 path too, not just resource_key."""
    import sys
    import types
    from datetime import datetime, timezone

    comp = S3ObservationSensorComponent(
        sensor_name="s3_1", asset_key="raw/orders", bucket_name="bucket-a",
    )
    defs = comp.build_defs(None)

    class _FakePaginator:
        def __init__(self, contents):
            self._contents = contents

        def paginate(self, **_kw):
            return [{"Contents": self._contents}]

    class _FakeS3Client:
        def __init__(self, contents):
            self._contents = contents

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _FakePaginator(self._contents)

    contents = [{"Size": 100, "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc)}]
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service, region_name=None: _FakeS3Client(contents)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    tick1 = _run(defs, {})
    assert len(tick1.asset_events) == 1

    tick2 = _run(defs, {}, cursor=tick1.cursor)
    assert tick2.asset_events == []
