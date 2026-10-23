"""FilesystemMonitorSensorComponent: one run per file by default, one run per batch with batch_into_one_run."""
import os
import time
from types import SimpleNamespace

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


def _sensor(mod, tmp_path, **attrs):
    component = mod.FilesystemMonitorSensorComponent(
        sensor_name="watch", directory_path=str(tmp_path), job_name="extract_job", **attrs
    )
    return list(component.build_defs(None).sensors)[0]


def _touch(tmp_path, name, mtime):
    p = tmp_path / name
    p.write_text("x")
    os.utime(p, (mtime, mtime))
    return p


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


def _no_active_runs(monkeypatch, instance, active=False):
    seen = {}

    def get_runs(filters=None, **kwargs):
        seen["filters"] = filters
        return [SimpleNamespace(run_id="r1")] if active else []

    monkeypatch.setattr(instance, "get_runs", get_runs)
    return seen


def test_default_mode_launches_one_run_per_new_file(mod, tmp_path, instance):
    now = time.time()
    for i in range(3):
        _touch(tmp_path, f"inv{i}.pdf", now - 100 + i)
    result = _tick(_sensor(mod, tmp_path), instance)
    assert len(result.run_requests) == 3


def test_batch_mode_launches_a_single_run_for_all_new_files(mod, tmp_path, instance):
    now = time.time()
    for i in range(3):
        _touch(tmp_path, f"inv{i}.pdf", now - 100 + i)
    result = _tick(_sensor(mod, tmp_path, batch_into_one_run=True), instance)
    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key.startswith("batch-")
    assert float(result.cursor) == pytest.approx(now - 98)  # the newest file


def test_batch_mode_only_picks_up_files_newer_than_the_cursor(mod, tmp_path, instance):
    now = time.time()
    _touch(tmp_path, "old.pdf", now - 200)
    sensor = _sensor(mod, tmp_path, batch_into_one_run=True)
    first = _tick(sensor, instance)
    assert len(first.run_requests) == 1

    again = _tick(sensor, instance, cursor=first.cursor)
    assert not again.run_requests and "No new files" in again.skip_reason.skip_message

    _touch(tmp_path, "new.pdf", now - 50)
    second = _tick(sensor, instance, cursor=first.cursor)
    assert len(second.run_requests) == 1
    assert second.run_requests[0].run_key != first.run_requests[0].run_key
    assert float(second.cursor) > float(first.cursor)


def test_batch_mode_waits_while_a_run_is_in_progress_without_losing_files(mod, tmp_path, instance, monkeypatch):
    _touch(tmp_path, "inv.pdf", time.time() - 100)
    sensor = _sensor(mod, tmp_path, batch_into_one_run=True)

    seen = _no_active_runs(monkeypatch, instance, active=True)
    waiting = _tick(sensor, instance)
    assert not waiting.run_requests and waiting.cursor is None  # cursor untouched: the file is still new
    assert "still in progress" in waiting.skip_reason.skip_message
    assert seen["filters"].job_name == "extract_job"
    assert dg.DagsterRunStatus.STARTED in seen["filters"].statuses and dg.DagsterRunStatus.SUCCESS not in seen["filters"].statuses

    _no_active_runs(monkeypatch, instance, active=False)
    assert len(_tick(sensor, instance).run_requests) == 1


def test_default_mode_does_not_look_at_running_jobs(mod, tmp_path, instance, monkeypatch):
    _touch(tmp_path, "inv.pdf", time.time() - 100)
    monkeypatch.setattr(instance, "get_runs", lambda *a, **k: pytest.fail("default mode must not query runs"))
    assert len(_tick(_sensor(mod, tmp_path), instance).run_requests) == 1


def test_batch_mode_requires_the_default_partition_mode(mod, tmp_path):
    component = mod.FilesystemMonitorSensorComponent(
        sensor_name="watch", directory_path=str(tmp_path), job_name="j", batch_into_one_run=True, partition_mode="static_partition"
    )
    with pytest.raises(ValueError, match="batch_into_one_run"):
        component.build_defs(None)


def test_a_case_insensitive_pattern_matches_file_names_like_the_designer_sends(mod, tmp_path, instance):
    now = time.time()
    _touch(tmp_path, "SCAN.PDF", now - 100)
    _touch(tmp_path, "notes.txt", now - 100)
    result = _tick(_sensor(mod, tmp_path, file_pattern=r"(?i).*\.(?:pdf|png)$", batch_into_one_run=True), instance)
    assert len(result.run_requests) == 1


def test_a_missing_directory_skips_instead_of_failing(mod, tmp_path, instance):
    result = _tick(_sensor(mod, tmp_path / "nope"), instance)
    assert "does not exist" in result.skip_reason.skip_message
