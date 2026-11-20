"""FilesystemMonitorSensorComponent: min_age_seconds/min_size_bytes/require_stable_size --
don't trigger on a file that's still being written or transferred in.

The state file lives in `tmp_path` directly, NOT inside the watched directory (`tmp_path/watch`)
-- otherwise the sensor would pick up its own state file as a "new file" to process (same
caveat as test_dynamic_partition_pruning.py).
"""
import os
import time

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


def _sensor(mod, watch_dir, **attrs):
    component = mod.FilesystemMonitorSensorComponent(
        sensor_name="watch", directory_path=str(watch_dir), job_name="extract_job", **attrs
    )
    return list(component.build_defs(None).sensors)[0]


def _write(watch_dir, name, content, mtime=None):
    p = watch_dir / name
    p.write_text(content)
    if mtime is not None:
        os.utime(p, (mtime, mtime))
    return p


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


# --- min_age_seconds ------------------------------------------------------------------------

def test_min_age_seconds_skips_a_too_young_file(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "data", mtime=time.time())  # just modified "now"
    sensor = _sensor(mod, watch_dir, min_age_seconds=60)
    result = _tick(sensor, instance)
    assert result.run_requests == []


def test_min_age_seconds_allows_an_old_enough_file(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "data", mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, min_age_seconds=60)
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1


def test_min_age_seconds_unset_does_not_filter(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "data", mtime=time.time())
    sensor = _sensor(mod, watch_dir)
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1


# --- min_size_bytes -------------------------------------------------------------------------

def test_min_size_bytes_skips_a_too_small_file(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "x", mtime=time.time() - 120)  # 1 byte
    sensor = _sensor(mod, watch_dir, min_size_bytes=100)
    result = _tick(sensor, instance)
    assert result.run_requests == []


def test_min_size_bytes_allows_a_big_enough_file(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "x" * 200, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, min_size_bytes=100)
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1


# --- require_stable_size ---------------------------------------------------------------------

def test_require_stable_size_holds_back_a_file_seen_for_the_first_time(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "stability.json")
    _write(watch_dir, "a.csv", "x" * 10, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, require_stable_size=True, file_stability_state_path=state_path)
    result = _tick(sensor, instance)
    assert result.run_requests == []  # first tick ever sees it -- nothing to compare against yet


def test_require_stable_size_fires_once_size_matches_the_prior_tick(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "stability.json")
    f = _write(watch_dir, "a.csv", "x" * 10, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, require_stable_size=True, file_stability_state_path=state_path)

    first = _tick(sensor, instance)
    assert first.run_requests == []

    # size unchanged since the first tick -- now stable.
    second = _tick(sensor, instance, cursor=first.cursor)
    assert len(second.run_requests) == 1


def test_require_stable_size_keeps_holding_back_a_still_growing_file(mod, tmp_path, watch_dir, instance):
    state_path = str(tmp_path / "stability.json")
    f = _write(watch_dir, "a.csv", "x" * 10, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, require_stable_size=True, file_stability_state_path=state_path)

    first = _tick(sensor, instance)
    assert first.run_requests == []

    # file grew between ticks (still being written) -- still not stable.
    f.write_text("x" * 50)
    os.utime(f, (time.time() - 120, time.time() - 120))
    second = _tick(sensor, instance, cursor=first.cursor)
    assert second.run_requests == []

    # now it stops growing -- stable on the NEXT tick after that.
    third = _tick(sensor, instance, cursor=second.cursor)
    assert len(third.run_requests) == 1


def test_require_stable_size_state_file_does_not_accumulate_processed_files(mod, tmp_path, watch_dir, instance):
    import json

    state_path = str(tmp_path / "stability.json")
    _write(watch_dir, "a.csv", "x" * 10, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir, require_stable_size=True, file_stability_state_path=state_path)

    first = _tick(sensor, instance)
    second = _tick(sensor, instance, cursor=first.cursor)
    assert len(second.run_requests) == 1  # fired and the cursor moved past it
    with open(state_path) as fh:
        assert "a.csv" in json.load(fh).popitem()[0]  # still tracked -- it WAS this tick's one candidate

    # A later file's own tick rebuilds state from scratch each time (one read-modify-write per
    # tick, keyed only by THAT tick's candidates) -- a.csv is no longer a candidate (its mtime is
    # now behind the moved cursor), so it drops out the moment any other candidate causes a write.
    _write(watch_dir, "b.csv", "y" * 10, mtime=time.time() - 120)
    third = _tick(sensor, instance, cursor=second.cursor)
    assert third.run_requests == []  # b.csv held back -- first time seen
    with open(state_path) as fh:
        state = json.load(fh)
    assert len(state) == 1 and "b.csv" in next(iter(state))


def test_require_stable_size_false_by_default_does_not_hold_anything_back(mod, watch_dir, instance):
    _write(watch_dir, "a.csv", "x" * 10, mtime=time.time() - 120)
    sensor = _sensor(mod, watch_dir)
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1
