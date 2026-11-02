"""Shared test helpers for DagsterRunsToPrometheusSensorComponent."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
from dagster._core.events import DagsterEventType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "dagster_runs_to_prometheus_sensor_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def real_run_status_sensor_context(instance: dg.DagsterInstance, sensor_name: str, status: str = "SUCCESS"):
    """Build a REAL dg.RunStatusSensorContext off a real completed run --
    not a hand-rolled stand-in -- so a test using this exercises the exact
    object shape (a real DagsterRun with no start_time/end_time attribute)
    that broke in production.
    """
    @dg.job
    def _probe_job():
        @dg.op
        def _o():
            return 1
        _o()

    result = _probe_job.execute_in_process(instance=instance, raise_on_error=False)
    event_type = DagsterEventType.RUN_SUCCESS if status == "SUCCESS" else DagsterEventType.RUN_FAILURE
    logs = instance.all_logs(result.run_id, of_type=event_type)
    assert logs, f"expected a {event_type} log entry"
    event = logs[0].dagster_event
    run = instance.get_run_by_id(result.run_id)
    return dg.build_run_status_sensor_context(
        sensor_name=sensor_name,
        dagster_event=event,
        dagster_instance=instance,
        dagster_run=run,
    )
