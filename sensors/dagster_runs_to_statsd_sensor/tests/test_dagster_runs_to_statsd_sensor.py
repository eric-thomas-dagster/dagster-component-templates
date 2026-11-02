"""Real, committed test for DagsterRunsToStatsdSensorComponent.

Same bug class as jobs/run_history_to_warehouse_job: `context.dagster_run` is
a real `DagsterRun` (confirmed via `DagsterRun._fields` against installed
dagster==1.13.24), which has no `start_time`/`end_time` attribute at all --
the shipped code read `run.start_time`/`run.end_time` directly off it,
raising `AttributeError` on every single run completion this sensor
observed. Only the `RunRecord` wrapper (from `get_run_records()`) has them.
"""
import socket

import dagster as dg
import pytest

from .conftest import load_component_module, real_run_status_sensor_context


@pytest.fixture
def mod():
    return load_component_module()


def test_push_uses_run_record_for_duration_not_dagster_run(mod, monkeypatch):
    sent = []

    class _FakeSocket:
        def __init__(self, *a, **k):
            pass
        def sendto(self, data, addr):
            sent.append((data, addr))
        def close(self):
            pass

    with dg.instance_for_test() as instance:
        # Build the real context BEFORE patching socket.socket -- Dagster's
        # own execution machinery uses real sockets internally (asyncio
        # self-pipe), so a blanket global patch during job execution
        # silently breaks event emission. Patch only the component module's
        # own `socket` reference, and only around the _push call itself.
        context = real_run_status_sensor_context(instance, "probe_statsd_sensor", status="SUCCESS")

        monkeypatch.setattr(mod.socket, "socket", lambda *a, **k: _FakeSocket())

        component = mod.DagsterRunsToStatsdSensorComponent(
            sensor_name="probe_statsd_sensor",
            statsd_host="localhost",
            statsd_port=8125,
            default_status="STOPPED",
        )
        # Must not raise AttributeError on context.dagster_run.start_time.
        component._push(context, status="success")

    assert sent, "expected at least one UDP packet to be sent"
    payloads = b"".join(data for data, _addr in sent).decode()
    assert "runs.total" in payloads
    assert "runs.duration_milliseconds" in payloads
