"""Real, committed tests for DagsterRunsToInfluxdbSensorComponent.

Same bug class as jobs/run_history_to_warehouse_job: `context.dagster_run`
is a real `DagsterRun`, which has no `start_time`/`end_time` attribute at
all (confirmed via `DagsterRun._fields` against installed dagster==1.13.24)
-- only the `RunRecord` wrapper from `get_run_records()` has them. The
shipped `_push_run` read `run.start_time`/`run.end_time` directly off
`context.dagster_run`, crashing with `AttributeError` on every run
completion. Real `RunStatusSensorContext`/`RunFailureSensorContext` objects
are built via `dg.build_run_status_sensor_context` + `.for_run_failure()`
(the officially documented unit-test mechanism) -- only the external
network call (`requests.post`) is mocked.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def success_context():
    with dg.instance_for_test() as instance:

        @dg.job
        def ok_job():
            @dg.op
            def o():
                return 1
            o()

        result = ok_job.execute_in_process(instance=instance)
        event = result.get_run_success_event()
        yield dg.build_run_status_sensor_context(
            sensor_name="probe", dagster_instance=instance,
            dagster_run=result.dagster_run, dagster_event=event,
        )


def test_push_run_success_does_not_crash_on_real_dagster_run(mod, success_context, monkeypatch):
    import requests as real_requests
    calls = []

    def fake_post(url, data, headers, params, timeout):
        calls.append(data)
        class R:
            def raise_for_status(self):
                pass
        return R()

    monkeypatch.setattr(real_requests, "post", fake_post)
    monkeypatch.setenv("INFLUX_URL_PROBE", "http://example.invalid:8086")
    monkeypatch.setenv("INFLUX_TOKEN_PROBE", "fake-token")

    component = mod.DagsterRunsToInfluxdbSensorComponent(
        sensor_name="probe_sensor", base_url_env_var="INFLUX_URL_PROBE",
        token_env_var="INFLUX_TOKEN_PROBE", org="org", bucket="bucket",
    )
    component._push_run(success_context, status="success")

    assert len(calls) == 1
    line = calls[0].decode("utf-8")
    assert "duration_seconds=" in line
    # A crash before this point would never produce a line at all.
    assert "finished_ts=" in line
