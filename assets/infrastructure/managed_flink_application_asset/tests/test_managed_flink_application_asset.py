"""Real, committed tests for ManagedFlinkApplicationAssetComponent.

Mocks only the external call: a fake kinesisanalyticsv2 client whose
describe_application/start_application/stop_application responses match the
real boto3 shapes (verified directly against the botocore reference, not
from memory -- see component.py's module docstring). Everything else
(state-machine logic, polling, Dagster asset wiring) is real.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class FakeFlinkClient:
    """`statuses` is the sequence of ApplicationStatus values describe_application
    returns on successive calls (the last value repeats once exhausted)."""

    def __init__(self, statuses, version_id=3):
        self.statuses = list(statuses)
        self.version_id = version_id
        self.calls = []

    def _next_status(self):
        if len(self.statuses) > 1:
            return self.statuses.pop(0)
        return self.statuses[0]

    def describe_application(self, ApplicationName):
        self.calls.append(("describe_application", {"ApplicationName": ApplicationName}))
        return {
            "ApplicationDetail": {
                "ApplicationStatus": self._next_status(),
                "ApplicationVersionId": self.version_id,
                "ApplicationARN": f"arn:aws:kinesisanalytics:us-east-1:123:application/{ApplicationName}",
            }
        }

    def start_application(self, **kwargs):
        self.calls.append(("start_application", kwargs))
        return {"OperationId": "op-start-1"}

    def stop_application(self, **kwargs):
        self.calls.append(("stop_application", kwargs))
        return {"OperationId": "op-stop-1"}


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_flink_client", lambda region_name, aws_profile: client)


def _comp(**kw):
    args = dict(asset_name="out", application_name="my-flink-app", region_name="us-east-1", poll_interval_seconds=0.001)
    args.update(kw)
    return mod.ManagedFlinkApplicationAssetComponent(**args)


def test_already_running_is_a_noop(monkeypatch):
    client = FakeFlinkClient(["RUNNING"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert not any(c[0] == "start_application" for c in client.calls)
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "none"
    assert meta["application_status"].value == "RUNNING"


def test_starts_from_ready_and_polls_until_running(monkeypatch):
    client = FakeFlinkClient(["READY", "STARTING", "STARTING", "RUNNING"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    start_calls = [c for c in client.calls if c[0] == "start_application"]
    assert len(start_calls) == 1
    assert start_calls[0][1]["RunConfiguration"]["FlinkRunConfiguration"]["AllowNonRestoredState"] is False
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "started"
    assert meta["operation_id"].value == "op-start-1"


def test_already_starting_just_polls_without_calling_start_again(monkeypatch):
    client = FakeFlinkClient(["STARTING", "RUNNING"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert not any(c[0] == "start_application" for c in client.calls)


def test_blocked_status_raises_a_clear_error(monkeypatch):
    client = FakeFlinkClient(["DELETING"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "DELETING" in str(failure.event_specific_data.error)


def test_rolled_back_during_poll_raises(monkeypatch):
    client = FakeFlinkClient(["READY", "STARTING", "ROLLED_BACK"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "rolled back" in str(failure.event_specific_data.error).lower()


def test_timeout_raises_when_stuck_starting(monkeypatch):
    client = FakeFlinkClient(["READY"] + ["STARTING"] * 5000)
    _patch_client(monkeypatch, client)
    comp = _comp(timeout_seconds=1)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "did not reach" in str(failure.event_specific_data.error)


def test_desired_state_stopped_calls_stop_and_polls_until_ready(monkeypatch):
    client = FakeFlinkClient(["RUNNING", "STOPPING", "READY"])
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="stopped")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    stop_calls = [c for c in client.calls if c[0] == "stop_application"]
    assert len(stop_calls) == 1
    assert stop_calls[0][1]["Force"] is False


def test_restore_configuration_is_threaded_through(monkeypatch):
    client = FakeFlinkClient(["READY", "RUNNING"])
    _patch_client(monkeypatch, client)
    comp = _comp(application_restore_type="RESTORE_FROM_CUSTOM_SNAPSHOT", snapshot_name="snap-1", allow_non_restored_state=True)
    dg.materialize(list(comp.build_defs(None).assets))
    start_call = next(c for c in client.calls if c[0] == "start_application")
    rc = start_call[1]["RunConfiguration"]
    assert rc["FlinkRunConfiguration"]["AllowNonRestoredState"] is True
    assert rc["ApplicationRestoreConfiguration"] == {
        "ApplicationRestoreType": "RESTORE_FROM_CUSTOM_SNAPSHOT",
        "SnapshotName": "snap-1",
    }


def test_custom_snapshot_without_snapshot_name_is_rejected():
    comp = _comp(application_restore_type="RESTORE_FROM_CUSTOM_SNAPSHOT")
    with pytest.raises(ValueError, match="snapshot_name"):
        comp.build_defs(None)


def test_invalid_desired_state_is_rejected():
    comp = _comp(desired_state="paused")
    with pytest.raises(ValueError, match="desired_state"):
        comp.build_defs(None)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(monkeypatch):
    client = FakeFlinkClient(["RUNNING"])
    _patch_client(monkeypatch, client)
    comp = _comp(partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
