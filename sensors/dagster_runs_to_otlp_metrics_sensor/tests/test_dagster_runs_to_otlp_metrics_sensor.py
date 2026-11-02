"""Real, committed tests for DagsterRunsToOtlpMetricsSensorComponent.

Same bug class as jobs/run_history_to_warehouse_job: `context.dagster_run`
is a real `DagsterRun`, which has no `start_time`/`end_time` attribute at
all (confirmed via `DagsterRun._fields` against installed dagster==1.13.24)
-- only the `RunRecord` wrapper from `get_run_records()` has them. The
shipped `_push` read `run.start_time`/`run.end_time` directly off
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


@pytest.fixture
def failure_context():
    with dg.instance_for_test() as instance:

        @dg.job
        def bad_job():
            @dg.op
            def b():
                raise Exception("boom")
            b()

        try:
            result = bad_job.execute_in_process(instance=instance, raise_on_error=False)
        except Exception:
            result = None
        if result is None:
            run = instance.get_runs(limit=1)[0]
            entry = next(iter(instance.all_logs(run.run_id, of_type=dg.DagsterEventType.RUN_FAILURE)))
            ctx = dg.build_run_status_sensor_context(
                sensor_name="probe", dagster_instance=instance,
                dagster_run=run, dagster_event=entry.dagster_event,
            ).for_run_failure()
        else:
            event = result.get_run_failure_event()
            ctx = dg.build_run_status_sensor_context(
                sensor_name="probe", dagster_instance=instance,
                dagster_run=result.dagster_run, dagster_event=event,
            ).for_run_failure()
        yield ctx


def test_push_success_does_not_crash_on_real_dagster_run(mod, success_context, monkeypatch):
    import requests as real_requests
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append(json)
        class R:
            def raise_for_status(self):
                pass
        return R()

    monkeypatch.setattr(real_requests, "post", fake_post)
    monkeypatch.setenv("OTLP_METRICS_ENDPOINT_PROBE", "https://example.invalid")

    component = mod.DagsterRunsToOtlpMetricsSensorComponent(
        sensor_name="probe_sensor", otlp_endpoint_env_var="OTLP_METRICS_ENDPOINT_PROBE",
    )
    component._push(success_context, status="success")

    assert len(calls) == 1
    metrics = calls[0]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
    duration_metric = next(m for m in metrics if m["name"] == "dagster.runs.duration_seconds")
    assert duration_metric["gauge"]["dataPoints"][0]["asDouble"] >= 0.0


def test_push_failure_does_not_crash_on_real_dagster_run(mod, failure_context, monkeypatch):
    import requests as real_requests
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append(json)
        class R:
            def raise_for_status(self):
                pass
        return R()

    monkeypatch.setattr(real_requests, "post", fake_post)
    monkeypatch.setenv("OTLP_METRICS_ENDPOINT_PROBE2", "https://example.invalid")

    component = mod.DagsterRunsToOtlpMetricsSensorComponent(
        sensor_name="probe_sensor2", otlp_endpoint_env_var="OTLP_METRICS_ENDPOINT_PROBE2",
    )
    component._push(failure_context, status="failure")

    assert len(calls) == 1
