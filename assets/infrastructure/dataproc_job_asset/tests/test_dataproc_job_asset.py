"""Real, committed tests for DataprocJobAssetComponent.

Mocks only the external call: a fake JobControllerClient whose
submit_job/get_job responses match the real google-cloud-dataproc v5 shapes
(verified directly against the installed `google.cloud.dataproc_v1` package
-- see component.py's module docstring). Everything else (job-body
building, polling loop, failure handling, Dagster asset wiring) is real.
"""
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


def _materialization_metadata(result):
    event = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION")
    return event.event_specific_data.materialization.metadata


def _job(job_id="job-123", state="PENDING", driver_output=None, details=None):
    return SimpleNamespace(
        reference=SimpleNamespace(job_id=job_id),
        status=SimpleNamespace(state=SimpleNamespace(name=state), details=details),
        driver_output_resource_uri=driver_output,
    )


class FakeJobClient:
    """`states` is the sequence of JobStatus.State values get_job returns on
    successive polls (the last value repeats once exhausted)."""

    def __init__(self, states, job_id="job-123", driver_output="gs://bucket/job-123/driveroutput"):
        self.states = list(states)
        self.job_id = job_id
        self.driver_output = driver_output
        self.calls = []

    def _next_state(self):
        if len(self.states) > 1:
            return self.states.pop(0)
        return self.states[0]

    def submit_job(self, project_id, region, job):
        self.calls.append(("submit_job", {"project_id": project_id, "region": region, "job": job}))
        return _job(job_id=self.job_id, state=self._next_state())

    def get_job(self, project_id, region, job_id):
        self.calls.append(("get_job", {"project_id": project_id, "region": region, "job_id": job_id}))
        state = self._next_state()
        details = "boom" if state in ("ERROR", "CANCELLED") else None
        return _job(job_id=job_id, state=state, driver_output=self.driver_output, details=details)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_job_client", lambda region, gcp_credentials_env_var: client)


def _comp(**kw):
    args = dict(
        asset_name="out", project_id="my-proj", region="us-central1", cluster_name="my-cluster",
        job_type="pyspark", main_python_file_uri="gs://bucket/job.py", poll_interval_seconds=0.001,
    )
    args.update(kw)
    return mod.DataprocJobAssetComponent(**args)


def test_pyspark_job_succeeds_after_polling(monkeypatch):
    client = FakeJobClient(["PENDING", "RUNNING", "RUNNING", "DONE"])
    _patch_client(monkeypatch, client)
    comp = _comp(args=["--date", "2026-11-26"])
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    submit_call = next(c for c in client.calls if c[0] == "submit_job")
    assert submit_call[1]["job"]["pyspark_job"]["main_python_file_uri"] == "gs://bucket/job.py"
    assert submit_call[1]["job"]["pyspark_job"]["args"] == ["--date", "2026-11-26"]
    meta = _materialization_metadata(result)
    assert meta["job_status"].value == "DONE"
    assert meta["driver_output_uri"].path == "gs://bucket/job-123/driveroutput"


def test_job_failure_raises(monkeypatch):
    client = FakeJobClient(["RUNNING", "ERROR"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success


def test_pyspark_requires_main_python_file_uri():
    comp = _comp(main_python_file_uri=None)
    with pytest.raises(ValueError, match="main_python_file_uri"):
        comp.build_defs(None)


def test_spark_job_type_requires_jar_or_class():
    comp = _comp(job_type="spark", main_python_file_uri=None)
    with pytest.raises(ValueError, match="main_jar_file_uri or main_class"):
        comp.build_defs(None)


def test_spark_job_builds_with_main_class(monkeypatch):
    client = FakeJobClient(["DONE"])
    _patch_client(monkeypatch, client)
    comp = _comp(
        job_type="spark", main_python_file_uri=None,
        main_class="com.example.Main", jar_file_uris=["gs://bucket/app.jar"],
    )
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    submit_call = next(c for c in client.calls if c[0] == "submit_job")
    spark_job = submit_call[1]["job"]["spark_job"]
    assert spark_job["main_class"] == "com.example.Main"
    assert spark_job["jar_file_uris"] == ["gs://bucket/app.jar"]
    assert "main_jar_file_uri" not in spark_job


def test_hive_job_requires_query_file_or_list():
    comp = _comp(job_type="hive", main_python_file_uri=None)
    with pytest.raises(ValueError, match="query_file_uri or query_list"):
        comp.build_defs(None)


def test_hive_job_with_inline_query_list(monkeypatch):
    client = FakeJobClient(["DONE"])
    _patch_client(monkeypatch, client)
    comp = _comp(job_type="hive", main_python_file_uri=None, query_list=["SELECT 1", "SELECT 2"])
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success

    submit_call = next(c for c in client.calls if c[0] == "submit_job")
    hive_job = submit_call[1]["job"]["hive_job"]
    assert hive_job["query_list"] == {"queries": ["SELECT 1", "SELECT 2"]}


def test_invalid_job_type_raises():
    comp = _comp(job_type="bogus", main_python_file_uri=None)
    with pytest.raises(ValueError, match="job_type"):
        comp.build_defs(None)


def test_timeout_raises(monkeypatch):
    client = FakeJobClient(["RUNNING"] * 50)
    _patch_client(monkeypatch, client)
    comp = _comp(timeout_seconds=1, poll_interval_seconds=0.001)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
