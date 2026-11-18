"""FilesystemMonitorSensorComponent: dynamic_partition_keep_days prunes partitions this
sensor registered once they're older than keep_days, via a local JSON state file independent
of the sensor's own cursor.

The state file lives in `tmp_path` directly, NOT inside the watched directory (`tmp_path/watch`)
-- otherwise the sensor would pick up its own state file as a "new file" to process.
"""
import json
import os
import time
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


@pytest.fixture()
def watch_dir(tmp_path):
    d = tmp_path / "watch"
    d.mkdir()
    return d


def _sensor(mod, watch_dir, state_path, **attrs):
    component = mod.FilesystemMonitorSensorComponent(
        sensor_name="watch", directory_path=str(watch_dir), job_name="extract_job",
        partition_mode="dynamic_partition", dynamic_partitions_name="files",
        dynamic_partition_state_path=state_path, **attrs,
    )
    return list(component.build_defs(None).sensors)[0]


def _touch(watch_dir, name, mtime):
    p = watch_dir / name
    p.write_text("x")
    os.utime(p, (mtime, mtime))
    return p


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


def _delete_requests(result):
    return [r for r in (result.dynamic_partitions_requests or []) if type(r).__name__ == "DeleteDynamicPartitionsRequest"]


def test_keep_days_unset_never_touches_the_state_file(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "state.json")
    _touch(watch_dir, "a.pdf", time.time() - 100)
    _tick(_sensor(mod, watch_dir, state_path), instance)
    assert not os.path.exists(state_path)


def test_new_partition_keys_are_recorded_in_state(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "state.json")
    _touch(watch_dir, "a.pdf", time.time() - 100)
    _tick(_sensor(mod, watch_dir, state_path, dynamic_partition_keep_days=7), instance)
    state = json.loads(open(state_path).read())
    assert "a" in state  # file_stem default partition_key_template


def test_partitions_older_than_keep_days_are_pruned(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    result = _tick(_sensor(mod, watch_dir, state_path, dynamic_partition_keep_days=7), instance)

    delete_reqs = _delete_requests(result)
    assert len(delete_reqs) == 1
    assert delete_reqs[0].partition_keys == ["stale_key"]
    remaining = json.loads(open(state_path).read())
    assert "stale_key" not in remaining


def test_partitions_within_keep_days_are_not_pruned(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "state.json")
    recent_seen = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"fresh_key": recent_seen}, f)

    result = _tick(_sensor(mod, watch_dir, state_path, dynamic_partition_keep_days=7), instance)

    assert _delete_requests(result) == []
    remaining = json.loads(open(state_path).read())
    assert "fresh_key" in remaining


def test_pruning_runs_even_on_a_quiet_tick_with_no_new_files(mod, tmp_path, watch_dir, instance):
    """A tick that finds no new files must still prune -- stale partitions age out regardless."""
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    result = _tick(_sensor(mod, watch_dir, state_path, dynamic_partition_keep_days=7), instance)

    assert not result.run_requests
    assert len(_delete_requests(result)) == 1
