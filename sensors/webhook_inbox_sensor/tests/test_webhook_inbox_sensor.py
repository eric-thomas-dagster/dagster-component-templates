"""Real, committed tests for WebhookInboxSensorComponent. Writes real JSONL lines to a real
tmp_path file (no mocking of file I/O or the sensor's own logic) and exercises the sensor
function through a real dagster SensorDefinition / build_sensor_context, the same way
sensors/salesforce_record_sensor/tests does.
"""
import json

import dagster as dg
import pytest


def _write_line(path, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def _build_component(mod, **overrides):
    defaults = dict(sensor_name="wh_sensor", inbox_path="unused.jsonl", job_name="process_webhook_job")
    defaults.update(overrides)
    return mod.WebhookInboxSensorComponent(**defaults)


def _run_sensor(component, cursor=None):
    """Invokes the sensor function directly (supported by Dagster for direct-invocation
    testing) rather than through a resolved repository -- that would additionally require a
    real job named after `job_name` with a real op matching `op_name`, and 'config' (this
    component's own op_name default, mirroring s3_monitor/sqs_monitor's convention) is itself
    a disallowed Dagster node name, so no such op could ever literally exist."""
    sensor_def = component.build_defs(None).sensors[0]
    ctx = dg.build_sensor_context(cursor=cursor)
    result = sensor_def(ctx)
    return result, ctx


# --- read_new_inbox_lines / cursor behavior ------------------------------------------------

def test_read_new_inbox_lines_returns_empty_when_file_missing(mod, tmp_path):
    records, cursor, skipped = mod.read_new_inbox_lines(str(tmp_path / "nope.jsonl"), 0)
    assert records == []
    assert cursor == 0
    assert skipped == 0


def test_read_new_inbox_lines_advances_cursor_across_ticks(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"received_at": "t1", "body": {"id": "a"}})
    _write_line(inbox, {"received_at": "t2", "body": {"id": "b"}})

    records, cursor, skipped = mod.read_new_inbox_lines(str(inbox), 0)
    assert len(records) == 2
    assert cursor == 2
    assert skipped == 0

    # Nothing new since last cursor.
    records2, cursor2, _ = mod.read_new_inbox_lines(str(inbox), cursor)
    assert records2 == []
    assert cursor2 == cursor

    # A third line arrives; only it is returned, cursor advances by one.
    _write_line(inbox, {"received_at": "t3", "body": {"id": "c"}})
    records3, cursor3, _ = mod.read_new_inbox_lines(str(inbox), cursor)
    assert len(records3) == 1
    assert records3[0]["body"]["id"] == "c"
    assert cursor3 == 3


def test_read_new_inbox_lines_skips_partial_last_line_without_crashing(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"received_at": "t1", "body": {"id": "a"}})
    # Simulate a write still in progress: no trailing newline.
    with open(inbox, "a", encoding="utf-8") as f:
        f.write('{"received_at": "t2", "body": {"id": "half-wri')

    records, cursor, skipped = mod.read_new_inbox_lines(str(inbox), 0)
    assert len(records) == 1  # only the complete first line
    assert cursor == 1  # does NOT count the partial line
    assert skipped == 0

    # Completing the write makes it visible on a later read.
    with open(inbox, "a", encoding="utf-8") as f:
        f.write('tten"}}\n')
    records2, cursor2, _ = mod.read_new_inbox_lines(str(inbox), cursor)
    assert len(records2) == 1
    assert cursor2 == 2


def test_read_new_inbox_lines_skips_malformed_complete_line_and_advances_past_it(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    with open(inbox, "w", encoding="utf-8") as f:
        f.write("{this is not valid json}\n")
    _write_line(inbox, {"received_at": "t2", "body": {"id": "ok"}})

    records, cursor, skipped = mod.read_new_inbox_lines(str(inbox), 0)
    assert len(records) == 1
    assert records[0]["body"]["id"] == "ok"
    assert cursor == 2  # advanced past BOTH lines, including the corrupt one
    assert skipped == 1


def test_read_new_inbox_lines_respects_max_events(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    for i in range(5):
        _write_line(inbox, {"body": {"id": str(i)}})

    records, cursor, _ = mod.read_new_inbox_lines(str(inbox), 0, max_events=2)
    assert len(records) == 2
    assert cursor == 2  # only advanced past what was actually consumed


# --- event_id_field / hash fallback ----------------------------------------------------

def test_event_id_for_uses_dotted_field(mod):
    assert mod._event_id_for({"job": {"id": "abc"}}, "job.id") == "abc"


def test_event_id_for_falls_back_to_stable_hash_when_field_absent(mod):
    body = {"status": "done"}
    h1 = mod._event_id_for(body, "id")
    h2 = mod._event_id_for(body, "id")
    assert h1 == h2
    assert h1.startswith("sha256:")
    assert mod._event_id_for({"status": "different"}, "id") != h1


# --- full sensor evaluation: run_config mode -------------------------------------------

def test_sensor_run_config_mode_yields_one_run_request_per_event(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"received_at": "t1", "remote_addr": "1.2.3.4", "path": "/webhook", "body": {"id": "job-1", "status": "done"}})
    _write_line(inbox, {"received_at": "t2", "remote_addr": "1.2.3.4", "path": "/webhook", "body": {"id": "job-2", "status": "done"}})

    component = _build_component(mod, inbox_path=str(inbox))
    result, ctx = _run_sensor(component)

    assert isinstance(result, dg.SensorResult)
    assert len(result.run_requests) == 2
    assert result.run_requests[0].run_key == "job-1"
    config = result.run_requests[0].run_config["ops"]["config"]["config"]
    assert config["body"] == {"id": "job-1", "status": "done"}
    assert config["remote_addr"] == "1.2.3.4"
    assert ctx.cursor == "2"


def test_sensor_skips_when_no_new_events(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    inbox.touch()
    component = _build_component(mod, inbox_path=str(inbox))
    result, ctx = _run_sensor(component)
    assert isinstance(result, dg.SensorResult)
    assert result.run_requests == []
    assert result.skip_reason is not None


def test_sensor_custom_op_name(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"body": {"id": "x"}})
    component = _build_component(mod, inbox_path=str(inbox), op_name="handle_webhook")
    result, _ctx = _run_sensor(component)
    assert "handle_webhook" in result.run_requests[0].run_config["ops"]


# --- full sensor evaluation: dynamic_partition mode -------------------------------------

def test_sensor_dynamic_partition_mode_registers_partitions(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"body": {"id": "evt-1"}})
    _write_line(inbox, {"body": {"id": "evt-2"}})

    component = _build_component(
        mod, inbox_path=str(inbox), partition_mode="dynamic_partition", dynamic_partitions_name="webhook_events"
    )
    result, _ctx = _run_sensor(component)

    assert len(result.run_requests) == 2
    assert {rr.partition_key for rr in result.run_requests} == {"evt-1", "evt-2"}
    assert len(result.dynamic_partitions_requests) == 1
    added = result.dynamic_partitions_requests[0]
    assert added.partitions_def_name == "webhook_events"
    assert set(added.partition_keys) == {"evt-1", "evt-2"}


def test_dynamic_partition_mode_requires_partitions_name(mod):
    component = _build_component(mod, inbox_path="unused.jsonl", partition_mode="dynamic_partition")
    with pytest.raises(ValueError, match="dynamic_partitions_name"):
        component.build_defs(None)


def test_invalid_partition_mode_raises(mod):
    component = _build_component(mod, inbox_path="unused.jsonl", partition_mode="bogus")
    with pytest.raises(ValueError, match="partition_mode"):
        component.build_defs(None)


# --- cursor resilience -------------------------------------------------------------------

def test_sensor_recovers_from_unparseable_cursor(mod, tmp_path):
    inbox = tmp_path / "inbox.jsonl"
    _write_line(inbox, {"body": {"id": "a"}})
    component = _build_component(mod, inbox_path=str(inbox))
    result, ctx = _run_sensor(component, cursor="not-a-number")
    assert len(result.run_requests) == 1
    assert ctx.cursor == "1"
