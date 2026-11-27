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
    def __init__(self, step_id="s-123", describe_states=None, clusters=None):
        self.calls = []
        self._step_id = step_id
        self._describe_states = list(describe_states or ["COMPLETED"])
        self._describe_calls = 0
        self._clusters = clusters if clusters is not None else [{"Id": "j-abc", "Name": "my-cluster"}]

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
    monkeypatch.setattr(resource, "_client", lambda: client)
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
