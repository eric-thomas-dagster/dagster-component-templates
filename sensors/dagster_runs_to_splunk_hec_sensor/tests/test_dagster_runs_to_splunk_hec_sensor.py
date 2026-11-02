"""Real, committed test for DagsterRunsToSplunkHecSensorComponent.

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

    def _fake_post(url, data=None, headers=None, timeout=None, verify=None):
        posted.append((url, data, headers))
        return _FakeResponse()

    with dg.instance_for_test() as instance:
        context = real_run_status_sensor_context(instance, "probe_splunk_sensor", status="SUCCESS")

        monkeypatch.setenv("PROBE_HEC_URL", "https://splunk.example.com:8088")
        monkeypatch.setenv("PROBE_HEC_TOKEN", "fake-token")
        monkeypatch.setattr("requests.post", _fake_post)

        component = mod.DagsterRunsToSplunkHecSensorComponent(
            sensor_name="probe_splunk_sensor",
            hec_url_env_var="PROBE_HEC_URL",
            hec_token_env_var="PROBE_HEC_TOKEN",
            sourcetype="dagster:run",
            source="dagster",
            location_label="test",
            default_status="STOPPED",
        )
        # Must not raise AttributeError on context.dagster_run.start_time.
        component._push_run(context, status="success")

    assert posted, "expected a real HTTP push"
    _url, body, _headers = posted[0]
    assert b"dagster_duration_seconds" in body
