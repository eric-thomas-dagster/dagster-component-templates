"""Real, committed tests for EmrStepAssetComponent.

Mocks only the external call (a fake boto3 EMR client's `.add_job_flow_steps()`
/`.describe_step()`/`.get_paginator("list_clusters")`, matching the real
response shapes verified against the botocore reference) -- everything else
(cluster-name resolution, polling loop, failure-detail surfacing, Dagster
asset wiring) is real.
"""
import dagster as dg
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


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_emr_client", lambda region_name, aws_profile: client)


def _comp(**kw):
    args = dict(asset_name="out", region_name="us-east-1", cluster_id="j-abc",
                args=["spark-submit", "s3://bucket/job.py"], poll_interval_seconds=0.001)
    args.update(kw)
    return mod.EmrStepAssetComponent(**args)


def test_successful_step_materializes_with_completed_state(monkeypatch):
    client = FakeEmrClient(describe_states=["PENDING", "RUNNING", "COMPLETED"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["state"].value == "COMPLETED"
    assert meta["step_id"].value == "s-123"
    assert meta["cluster_id"].value == "j-abc"

    add_call = next(c for name, c in client.calls if name == "add_job_flow_steps")
    assert add_call["JobFlowId"] == "j-abc"
    step = add_call["Steps"][0]
    assert step["HadoopJarStep"]["Jar"] == "command-runner.jar"
    assert step["HadoopJarStep"]["Args"] == ["spark-submit", "s3://bucket/job.py"]
    assert step["ActionOnFailure"] == "CONTINUE"


def test_failed_step_raises_with_failure_details(monkeypatch):
    client = FakeEmrClient(describe_states=["RUNNING", "FAILED"])
    _patch_client(monkeypatch, client)
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    err = str(failure.event_specific_data.error)
    assert "FAILED" in err
    assert "step failed" in err


def test_failed_step_does_not_raise_when_disabled(monkeypatch):
    client = FakeEmrClient(describe_states=["FAILED"])
    _patch_client(monkeypatch, client)
    comp = _comp(fail_on_step_failure=False)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert mats[0].event_specific_data.materialization.metadata["state"].value == "FAILED"


def test_wait_for_completion_false_skips_polling(monkeypatch):
    client = FakeEmrClient()
    _patch_client(monkeypatch, client)
    comp = _comp(wait_for_completion=False)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    assert not any(name == "describe_step" for name, _ in client.calls)
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert "state" not in mats[0].event_specific_data.materialization.metadata


def test_cluster_name_resolves_to_a_single_matching_cluster(monkeypatch):
    client = FakeEmrClient(clusters=[{"Id": "j-xyz", "Name": "analytics-cluster"}])
    _patch_client(monkeypatch, client)
    comp = _comp(cluster_id=None, cluster_name="analytics-cluster")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    add_call = next(c for name, c in client.calls if name == "add_job_flow_steps")
    assert add_call["JobFlowId"] == "j-xyz"


def test_cluster_name_with_no_match_raises(monkeypatch):
    client = FakeEmrClient(clusters=[])
    _patch_client(monkeypatch, client)
    comp = _comp(cluster_id=None, cluster_name="missing-cluster")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "missing-cluster" in str(failure.event_specific_data.error)


def test_cluster_name_with_multiple_matches_raises(monkeypatch):
    client = FakeEmrClient(clusters=[{"Id": "j-1", "Name": "dupe"}, {"Id": "j-2", "Name": "dupe"}])
    _patch_client(monkeypatch, client)
    comp = _comp(cluster_id=None, cluster_name="dupe")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "Multiple active EMR clusters" in str(failure.event_specific_data.error)


def test_neither_cluster_id_nor_cluster_name_is_rejected():
    comp = _comp(cluster_id=None, cluster_name=None)
    with pytest.raises(ValueError, match="cluster_id or cluster_name"):
        comp.build_defs(None)


def test_main_class_and_properties_are_passed_through(monkeypatch):
    client = FakeEmrClient()
    _patch_client(monkeypatch, client)
    comp = _comp(jar="s3://bucket/my.jar", main_class="com.example.Main", properties={"k": "v"})
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    add_call = next(c for name, c in client.calls if name == "add_job_flow_steps")
    step = add_call["Steps"][0]["HadoopJarStep"]
    assert step["Jar"] == "s3://bucket/my.jar"
    assert step["MainClass"] == "com.example.Main"
    assert step["Properties"] == [{"Key": "k", "Value": "v"}]
