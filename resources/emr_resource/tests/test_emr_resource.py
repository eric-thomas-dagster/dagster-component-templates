"""Real, committed tests for EmrResource.submit_step_and_wait.

Mocks only the external call (a fake boto3 EMR client's `.add_job_flow_steps()`
/`.describe_step()`/`.get_paginator("list_clusters")`, matching the real
response shapes verified against the botocore reference) -- everything else
(cluster-name resolution, polling loop, failure-detail surfacing) is real.
"""
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return iter(self._pages)


class FakeEmrClient:
    def __init__(self, step_id="s-123", describe_states=None, clusters=None, cluster_states=None, job_flow_id="j-new"):
        self.calls = []
        self._step_id = step_id
        self._describe_states = list(describe_states or ["COMPLETED"])
        self._describe_calls = 0
        self._clusters = clusters if clusters is not None else [{"Id": "j-abc", "Name": "my-cluster"}]
        self._cluster_states = list(cluster_states or ["RUNNING"])
        self._cluster_describe_calls = 0
        self._job_flow_id = job_flow_id

    def run_job_flow(self, **kwargs):
        self.calls.append(("run_job_flow", kwargs))
        return {"JobFlowId": self._job_flow_id}

    def terminate_job_flows(self, **kwargs):
        self.calls.append(("terminate_job_flows", kwargs))
        return {}

    def describe_cluster(self, **kwargs):
        self.calls.append(("describe_cluster", kwargs))
        state = self._cluster_states[min(self._cluster_describe_calls, len(self._cluster_states) - 1)]
        self._cluster_describe_calls += 1
        status = {"State": state}
        if state == "TERMINATED_WITH_ERRORS":
            status["StateChangeReason"] = {"Message": "bootstrap failure"}
        return {"Cluster": {"Status": status}}

    def add_job_flow_steps(self, **kwargs):
        self.calls.append(("add_job_flow_steps", kwargs))
        return {"StepIds": [self._step_id]}

    def describe_step(self, **kwargs):
        self.calls.append(("describe_step", kwargs))
        state = self._describe_states[min(self._describe_calls, len(self._describe_states) - 1)]
        self._describe_calls += 1
        status = {"State": state}
        if state in mod._FAILURE_STATES:
            status["FailureDetails"] = {"Reason": "SCRIPT_RUNNER_ERROR", "Message": "step failed", "LogFile": "s3://bucket/logs/s-123"}
        return {"Step": {"Status": status}}

    def get_paginator(self, name):
        assert name == "list_clusters"
        return _FakePaginator([{"Clusters": self._clusters}])


def _resource(monkeypatch, client):
    resource = mod.EmrResource()
    monkeypatch.setattr(mod.EmrResource, "_client", lambda self: client)
    return resource


def test_successful_step_returns_completed_state(monkeypatch):
    client = FakeEmrClient(describe_states=["PENDING", "RUNNING", "COMPLETED"])
    resource = _resource(monkeypatch, client)
    result = resource.submit_step_and_wait(
        cluster_id="j-abc", args=["spark-submit", "s3://bucket/job.py"], poll_interval_seconds=0.001,
    )
    assert result == {"cluster_id": "j-abc", "step_id": "s-123", "state": "COMPLETED"}

    add_call = next(c for name, c in client.calls if name == "add_job_flow_steps")
    assert add_call["JobFlowId"] == "j-abc"
    step = add_call["Steps"][0]
    assert step["HadoopJarStep"]["Jar"] == "command-runner.jar"
    assert step["HadoopJarStep"]["Args"] == ["spark-submit", "s3://bucket/job.py"]
    assert step["ActionOnFailure"] == "CONTINUE"


def test_failed_step_raises_with_failure_details(monkeypatch):
    client = FakeEmrClient(describe_states=["RUNNING", "FAILED"])
    resource = _resource(monkeypatch, client)
    with pytest.raises(RuntimeError, match="FAILED"):
        resource.submit_step_and_wait(cluster_id="j-abc", args=["x"], poll_interval_seconds=0.001)


def test_failed_step_does_not_raise_when_disabled(monkeypatch):
    client = FakeEmrClient(describe_states=["FAILED"])
    resource = _resource(monkeypatch, client)
    result = resource.submit_step_and_wait(
        cluster_id="j-abc", args=["x"], poll_interval_seconds=0.001, fail_on_step_failure=False,
    )
    assert result["state"] == "FAILED"


def test_wait_for_completion_false_skips_polling(monkeypatch):
    client = FakeEmrClient()
    resource = _resource(monkeypatch, client)
    result = resource.submit_step_and_wait(cluster_id="j-abc", args=["x"], wait_for_completion=False)
    assert not any(name == "describe_step" for name, _ in client.calls)
    assert "state" not in result


def test_cluster_name_resolves_to_a_single_matching_cluster(monkeypatch):
    client = FakeEmrClient(clusters=[{"Id": "j-xyz", "Name": "analytics-cluster"}])
    resource = _resource(monkeypatch, client)
    result = resource.submit_step_and_wait(cluster_name="analytics-cluster", args=["x"], poll_interval_seconds=0.001)
    assert result["cluster_id"] == "j-xyz"


def test_cluster_name_with_no_match_raises(monkeypatch):
    client = FakeEmrClient(clusters=[])
    resource = _resource(monkeypatch, client)
    with pytest.raises(RuntimeError, match="missing-cluster"):
        resource.submit_step_and_wait(cluster_name="missing-cluster", args=["x"])


def test_cluster_name_with_multiple_matches_raises(monkeypatch):
    client = FakeEmrClient(clusters=[{"Id": "j-1", "Name": "dupe"}, {"Id": "j-2", "Name": "dupe"}])
    resource = _resource(monkeypatch, client)
    with pytest.raises(RuntimeError, match="Multiple active EMR clusters"):
        resource.submit_step_and_wait(cluster_name="dupe", args=["x"])


def test_neither_cluster_id_nor_cluster_name_is_rejected():
    resource = mod.EmrResource()
    with pytest.raises(ValueError, match="cluster_id or cluster_name"):
        resource.submit_step_and_wait(args=["x"])


def test_create_cluster_and_wait_returns_ready_state(monkeypatch):
    client = FakeEmrClient(cluster_states=["STARTING", "BOOTSTRAPPING", "WAITING"], job_flow_id="j-new")
    resource = _resource(monkeypatch, client)
    result = resource.create_cluster_and_wait(
        name="my-cluster",
        instances={"InstanceCount": 3, "MasterInstanceType": "m5.xlarge", "SlaveInstanceType": "m5.xlarge"},
        release_label="emr-6.15.0",
        applications=["Spark"],
        poll_interval_seconds=0.001,
    )
    assert result == {"cluster_id": "j-new", "state": "WAITING"}

    run_call = next(c for name, c in client.calls if name == "run_job_flow")
    assert run_call["Name"] == "my-cluster"
    assert run_call["ReleaseLabel"] == "emr-6.15.0"
    assert run_call["Applications"] == [{"Name": "Spark"}]
    assert run_call["Instances"]["InstanceCount"] == 3
    assert run_call["VisibleToAllUsers"] is True


def test_create_cluster_and_wait_raises_on_terminated_with_errors(monkeypatch):
    client = FakeEmrClient(cluster_states=["STARTING", "TERMINATED_WITH_ERRORS"])
    resource = _resource(monkeypatch, client)
    with pytest.raises(RuntimeError, match="bootstrap failure"):
        resource.create_cluster_and_wait(name="x", instances={}, poll_interval_seconds=0.001)


def test_create_cluster_and_wait_for_running_false_skips_polling(monkeypatch):
    client = FakeEmrClient()
    resource = _resource(monkeypatch, client)
    result = resource.create_cluster_and_wait(name="x", instances={}, wait_for_running=False)
    assert result == {"cluster_id": "j-new"}
    assert not any(name == "describe_cluster" for name, _ in client.calls)


def test_terminate_cluster_by_id_polls_to_terminated(monkeypatch):
    client = FakeEmrClient(cluster_states=["TERMINATING", "TERMINATED"])
    resource = _resource(monkeypatch, client)
    result = resource.terminate_cluster(cluster_id="j-abc", poll_interval_seconds=0.001)
    assert result == {"cluster_id": "j-abc", "state": "TERMINATED"}
    term_call = next(c for name, c in client.calls if name == "terminate_job_flows")
    assert term_call["JobFlowIds"] == ["j-abc"]


def test_terminate_cluster_by_name_resolves_then_terminates(monkeypatch):
    client = FakeEmrClient(clusters=[{"Id": "j-xyz", "Name": "analytics-cluster"}], cluster_states=["TERMINATED"])
    resource = _resource(monkeypatch, client)
    result = resource.terminate_cluster(cluster_name="analytics-cluster", poll_interval_seconds=0.001)
    assert result["cluster_id"] == "j-xyz"


def test_terminate_cluster_without_wait_skips_polling(monkeypatch):
    client = FakeEmrClient()
    resource = _resource(monkeypatch, client)
    result = resource.terminate_cluster(cluster_id="j-abc", wait_for_termination=False)
    assert result == {"cluster_id": "j-abc"}
    assert not any(name == "describe_cluster" for name, _ in client.calls)


def test_terminate_cluster_neither_id_nor_name_is_rejected():
    resource = mod.EmrResource()
    with pytest.raises(ValueError, match="cluster_id or cluster_name"):
        resource.terminate_cluster()


def test_main_class_and_properties_are_passed_through(monkeypatch):
    client = FakeEmrClient()
    resource = _resource(monkeypatch, client)
    resource.submit_step_and_wait(
        cluster_id="j-abc", jar="s3://bucket/my.jar", main_class="com.example.Main",
        properties={"k": "v"}, args=["x"], poll_interval_seconds=0.001,
    )
    add_call = next(c for name, c in client.calls if name == "add_job_flow_steps")
    step = add_call["Steps"][0]["HadoopJarStep"]
    assert step["Jar"] == "s3://bucket/my.jar"
    assert step["MainClass"] == "com.example.Main"
    assert step["Properties"] == [{"Key": "k", "Value": "v"}]


def test_component_registers_resource():
    component = mod.EmrResourceComponent(region_name="us-east-1")
    defs = component.build_defs(None)
    assert isinstance(defs.resources["emr_resource"], mod.EmrResource)
    assert defs.resources["emr_resource"].region_name == "us-east-1"
