"""Real, committed tests for DataprocResource.

Mocks only the external call: fake ClusterControllerClient/JobControllerClient whose
responses match the real google-cloud-dataproc v5 shapes (same fakes proven against the
real client when this logic lived in the now-removed dataproc_cluster_asset/
dataproc_job_asset components). Everything else (cluster-config/job-body building, the
idempotent present/absent checks, the polling loop, failure handling) is real.
"""
from types import SimpleNamespace

import pytest

from .conftest import load_component_module

mod = load_component_module()


# ---- cluster lifecycle -------------------------------------------------

def _cluster(name="my-cluster", state="RUNNING"):
    return SimpleNamespace(cluster_name=name, status=SimpleNamespace(state=SimpleNamespace(name=state)))


class FakeClusterClient:
    def __init__(self, existing=None):
        self.existing = existing
        self.calls = []

    def get_cluster(self, project_id, region, cluster_name):
        self.calls.append(("get_cluster", project_id, region, cluster_name))
        if self.existing is None:
            from google.api_core.exceptions import NotFound
            raise NotFound("no such cluster")
        return self.existing

    def create_cluster(self, project_id, region, cluster):
        self.calls.append(("create_cluster", project_id, region, cluster))
        created = _cluster(name=cluster["cluster_name"])

        class _Op:
            def result(_self, timeout):
                return created
        return _Op()

    def delete_cluster(self, project_id, region, cluster_name):
        self.calls.append(("delete_cluster", project_id, region, cluster_name))

        class _Op:
            def result(_self, timeout):
                return None
        return _Op()


def _resource(**kw):
    return mod.DataprocResource(**kw)


def test_ensure_cluster_creates_when_missing(monkeypatch):
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    result = r.ensure_cluster("proj", "us-central1", "my-cluster", cluster_config={"num_workers": 4})
    assert result == {"action": "created", "cluster_status": "RUNNING"}
    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    assert create_call[3]["config"]["worker_config"]["num_instances"] == 4


def test_ensure_cluster_is_idempotent_when_present(monkeypatch):
    client = FakeClusterClient(existing=_cluster(state="RUNNING"))
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    result = r.ensure_cluster("proj", "us-central1", "my-cluster")
    assert result == {"action": "none", "cluster_status": "RUNNING"}
    assert not any(c[0] == "create_cluster" for c in client.calls)


def test_delete_cluster_is_idempotent_when_absent(monkeypatch):
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    result = r.delete_cluster("proj", "us-central1", "my-cluster")
    assert result == {"action": "none"}
    assert not any(c[0] == "delete_cluster" for c in client.calls)


def test_delete_cluster_deletes_when_present(monkeypatch):
    client = FakeClusterClient(existing=_cluster())
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    result = r.delete_cluster("proj", "us-central1", "my-cluster")
    assert result == {"action": "deleted"}
    assert any(c[0] == "delete_cluster" for c in client.calls)


def test_ensure_cluster_network_tags_accepts_csv_string(monkeypatch):
    """Regression check for the csv-vs-list network_tags bug fixed earlier this session."""
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    r.ensure_cluster("proj", "us-central1", "my-cluster", cluster_config={"network_tags": ["a", "b"]})
    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    assert create_call[3]["config"]["gce_cluster_config"]["tags"] == ["a", "b"]


# ---- job submission -----------------------------------------------------

def _job(job_id="job-123", state="PENDING", driver_output=None, details=None):
    return SimpleNamespace(
        reference=SimpleNamespace(job_id=job_id),
        status=SimpleNamespace(state=SimpleNamespace(name=state), details=details),
        driver_output_resource_uri=driver_output,
    )


class FakeJobClient:
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


def test_submit_pyspark_job_succeeds_after_polling(monkeypatch):
    client = FakeJobClient(["PENDING", "RUNNING", "RUNNING", "DONE"])
    monkeypatch.setattr(mod.DataprocResource, "_job_client", lambda self, region: client)
    r = _resource()

    result = r.submit_job_and_wait(
        "proj", "us-central1", "my-cluster", "pyspark",
        job_config={"main_python_file_uri": "gs://bucket/job.py", "args": ["--date", "2026-11-26"]},
        poll_interval_seconds=0.001,
    )
    assert result["job_id"] == "job-123"
    assert result["job_status"] == "DONE"
    assert result["driver_output_uri"] == "gs://bucket/job-123/driveroutput"

    submit_call = next(c for c in client.calls if c[0] == "submit_job")
    assert submit_call[1]["job"]["pyspark_job"]["main_python_file_uri"] == "gs://bucket/job.py"
    assert submit_call[1]["job"]["pyspark_job"]["args"] == ["--date", "2026-11-26"]


def test_submit_job_raises_on_error_state(monkeypatch):
    client = FakeJobClient(["RUNNING", "ERROR"])
    monkeypatch.setattr(mod.DataprocResource, "_job_client", lambda self, region: client)
    r = _resource()

    with pytest.raises(RuntimeError, match="ERROR"):
        r.submit_job_and_wait(
            "proj", "us-central1", "my-cluster", "spark",
            job_config={"main_class": "com.example.Main", "jar_file_uris": ["gs://bucket/app.jar"]},
            poll_interval_seconds=0.001,
        )


def test_submit_job_times_out(monkeypatch):
    client = FakeJobClient(["RUNNING", "RUNNING", "RUNNING"])
    monkeypatch.setattr(mod.DataprocResource, "_job_client", lambda self, region: client)
    r = _resource()

    with pytest.raises(TimeoutError):
        r.submit_job_and_wait(
            "proj", "us-central1", "my-cluster", "pyspark",
            job_config={"main_python_file_uri": "gs://bucket/job.py"},
            poll_interval_seconds=0.001, timeout_seconds=0,
        )


def test_submit_job_requires_main_file_for_pyspark():
    r = _resource()
    with pytest.raises(ValueError, match="main_python_file_uri"):
        r.submit_job_and_wait("proj", "us-central1", "my-cluster", "pyspark", job_config={})


def test_submit_job_hive_with_query_list(monkeypatch):
    client = FakeJobClient(["DONE"])
    monkeypatch.setattr(mod.DataprocResource, "_job_client", lambda self, region: client)
    r = _resource()

    result = r.submit_job_and_wait(
        "proj", "us-central1", "my-cluster", "hive",
        job_config={"query_list": ["SELECT 1"]},
        poll_interval_seconds=0.001,
    )
    assert result["job_status"] == "DONE"
    submit_call = next(c for c in client.calls if c[0] == "submit_job")
    assert submit_call[1]["job"]["hive_job"]["query_list"] == {"queries": ["SELECT 1"]}


# ---- batch create ----------------------------------------------------

class FakeBatchClient:
    def __init__(self, final_state="SUCCEEDED", state_message=""):
        self.calls = []
        self._final_state = final_state
        self._state_message = state_message

    def create_batch(self, parent, batch, batch_id):
        self.calls.append(("create_batch", parent, batch, batch_id))
        final_state = self._final_state
        state_message = self._state_message

        class _Op:
            def result(_self, timeout):
                return SimpleNamespace(state=SimpleNamespace(name=final_state), state_message=state_message)
        return _Op()


def test_create_batch_and_wait_returns_succeeded_state(monkeypatch):
    client = FakeBatchClient(final_state="SUCCEEDED")
    monkeypatch.setattr(mod.DataprocResource, "_batch_client", lambda self, region: client)
    r = _resource()

    result = r.create_batch_and_wait(
        "proj", "us-central1", "my-batch",
        batch_config={"pyspark_batch": {"main_python_file_uri": "gs://bucket/job.py"}},
    )
    assert result == {"batch_id": "my-batch", "state": "SUCCEEDED"}

    create_call = next(c for c in client.calls if c[0] == "create_batch")
    assert create_call[1] == "projects/proj/regions/us-central1"
    assert create_call[3] == "my-batch"


def test_create_batch_and_wait_raises_on_failed(monkeypatch):
    client = FakeBatchClient(final_state="FAILED", state_message="boom")
    monkeypatch.setattr(mod.DataprocResource, "_batch_client", lambda self, region: client)
    r = _resource()

    with pytest.raises(RuntimeError, match="boom"):
        r.create_batch_and_wait("proj", "us-central1", "my-batch", batch_config={"spark_batch": {"main_class": "Main"}})


# ---- cluster_config_overrides -------------------------------------------

def test_ensure_cluster_flat_fields_only_unchanged(monkeypatch):
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    r.ensure_cluster("proj", "us-central1", "my-cluster", cluster_config={"num_workers": 3})
    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    config = create_call[3]["config"]
    assert config["worker_config"]["num_instances"] == 3
    assert "autoscaling_config" not in config


def test_ensure_cluster_overrides_only_adds_real_clusterconfig_fields(monkeypatch):
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    r.ensure_cluster(
        "proj", "us-central1", "my-cluster",
        cluster_config_overrides={"autoscaling_config": {"policy_uri": "my-policy"}},
    )
    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    config = create_call[3]["config"]
    assert config["autoscaling_config"] == {"policy_uri": "my-policy"}
    # flat-field defaults are still applied underneath the override
    assert config["worker_config"]["num_instances"] == 2


def test_ensure_cluster_overrides_win_on_conflict_with_flat_fields(monkeypatch):
    client = FakeClusterClient(existing=None)
    monkeypatch.setattr(mod.DataprocResource, "_cluster_client", lambda self, region: client)
    r = _resource()

    r.ensure_cluster(
        "proj", "us-central1", "my-cluster",
        cluster_config={"num_workers": 2, "master_machine_type": "n2-standard-4"},
        cluster_config_overrides={"master_config": {"machine_type_uri": "n2-standard-16"}},
    )
    create_call = next(c for c in client.calls if c[0] == "create_cluster")
    config = create_call[3]["config"]
    # override wins over the flat field's generated value
    assert config["master_config"]["machine_type_uri"] == "n2-standard-16"
    # sibling keys within the same nested dict, not touched by the override, survive the merge
    assert config["master_config"]["num_instances"] == 1
    # unrelated flat field is untouched
    assert config["worker_config"]["num_instances"] == 2


# ---- component registration ----------------------------------------------

def test_component_registers_resource_under_key():
    comp = mod.DataprocResourceComponent(resource_key="my_dataproc")
    defs = comp.build_defs(context=None)
    resource = defs.resources["my_dataproc"]
    assert isinstance(resource, mod.DataprocResource)
