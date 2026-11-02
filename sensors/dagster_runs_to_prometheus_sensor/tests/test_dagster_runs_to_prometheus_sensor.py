"""Real, committed test for DagsterRunsToPrometheusSensorComponent.

Same bug class as jobs/run_history_to_warehouse_job: `context.dagster_run` is
a real `DagsterRun` (confirmed via `DagsterRun._fields` against installed
dagster==1.13.24), which has no `start_time`/`end_time` attribute at all --
the shipped code read `run.start_time`/`run.end_time` directly off it,
raising `AttributeError` on every single run completion this sensor
observed. Only the `RunRecord` wrapper (from `get_run_records()`) has them.
"""
import dagster as dg
import pytest

from .conftest import load_component_module, real_run_status_sensor_context


@pytest.fixture
def mod():
    return load_component_module()


def test_push_run_uses_run_record_for_duration_not_dagster_run(mod, monkeypatch):
    posted = []

    class _FakeResponse:
        def raise_for_status(self):
            pass

    def _fake_post(url, data=None, headers=None, timeout=None):
        posted.append((url, data, headers))
        return _FakeResponse()

    with dg.instance_for_test() as instance:
        context = real_run_status_sensor_context(instance, "probe_prometheus_sensor", status="SUCCESS")

        monkeypatch.setenv("PROBE_PROMETHEUS_URL", "http://localhost:9091")
        monkeypatch.setattr("requests.post", _fake_post)

        component = mod.DagsterRunsToPrometheusSensorComponent(
            sensor_name="probe_prometheus_sensor",
            base_url_env_var="PROBE_PROMETHEUS_URL",
            target_kind="pushgateway",
            push_job="probe_job",
            location_label="test",
            default_status="STOPPED",
        )
        # Must not raise AttributeError on context.dagster_run.start_time.
        component._push_run(context, status="success")

    assert posted, "expected a real HTTP push"
    _url, body, _headers = posted[0]
    assert b"dagster_run_duration_seconds" in body
    assert b"dagster_run_last_finished_timestamp" in body
