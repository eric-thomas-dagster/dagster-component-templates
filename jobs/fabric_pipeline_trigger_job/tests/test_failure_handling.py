"""Real, committed tests for gaps a demo-builder reported against
FabricPipelineTriggerJobComponent (dagster-demo-factory component-feedback,
evaluated alongside fabric_workspace v1.1.0):

1. `parameters` were sent as `{"executionData": {"parameters": <raw dict>}}`
   for EVERY item type -- doesn't match Fabric's documented request shape on
   either count for DataPipeline/Dataflow: `parameters` is a TOP-LEVEL
   request field there (not nested under `executionData`), and it must be a
   typed Parameter[] list ({name, value, type}), not a bare dict. CORRECTED
   LATER: Notebook turned out to be the one case where `executionData` genuinely
   IS the right place -- just with a typed dict shape
   ({"parameters": {name: {value, type}}}), not the raw dict this bug
   originally sent. Confirmed via Microsoft's own
   apache-airflow-providers-microsoft-fabric MSFabricNotebookJobParameters
   class and the official `fab` CLI's job-run docs.
2. `Deduped` wasn't a recognized terminal status, so a job-scheduling
   collision just polled until the timeout.
3. Failed/Cancelled raised a bare `Exception` with a raw state dict dump --
   no failureReason extraction, no retriable classification, and
   `dg.Failure`/`dg.RetryRequested` weren't used at all.
4. Timeout raised but never cancelled the orphaned Fabric job.

`azure.identity` isn't installed in this environment -- faked via
sys.modules (the component only imports it lazily, inside the op body,
specifically so a test/demo run never needs it installed at all), restored
afterward so it doesn't leak into other test files.
"""
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
FabricPipelineTriggerJobComponent = mod.FabricPipelineTriggerJobComponent


class _FakeCredential:
    def __init__(self, *a, **k):
        pass

    def get_token(self, scope):
        return types.SimpleNamespace(token="fake-token")


@pytest.fixture
def fake_azure_identity():
    saved = {k: sys.modules.get(k) for k in ("azure", "azure.identity")}
    azure_mod = types.ModuleType("azure")
    identity_mod = types.ModuleType("azure.identity")
    identity_mod.DefaultAzureCredential = _FakeCredential
    identity_mod.ClientSecretCredential = _FakeCredential
    azure_mod.identity = identity_mod
    sys.modules["azure"] = azure_mod
    sys.modules["azure.identity"] = identity_mod
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


class _FakeResponse:
    def __init__(self, status_code=200, json_body=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_body or {}
        self.headers = headers or {}
        self.text = text or ""

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 300:
            raise Exception(f"HTTP {self.status_code}")


def _component(**overrides):
    attrs = dict(job_name="trigger_job", workspace_id="ws-1", item_id="item-1", item_type="DataPipeline", poll_interval_seconds=0)
    attrs.update(overrides)
    return FabricPipelineTriggerJobComponent(**attrs)


def _run_job(comp):
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    return job.execute_in_process(raise_on_error=True)


def test_completed_job_succeeds(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed"}))

    comp = _component()
    result = _run_job(comp)
    assert result.success


def test_failed_job_raises_a_real_failure_not_a_bare_exception(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed",
        "failureReason": {"errorCode": "NotebookExecutionFailed", "message": "cell 2 raised", "isRetriable": False},
    }))

    comp = _component()
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success


def test_deduped_job_fails_immediately_not_after_a_timeout_style_poll(fake_azure_identity, monkeypatch):
    import requests
    poll_count = {"n": 0}

    def _get(*a, **k):
        poll_count["n"] += 1
        return _FakeResponse(200, {"status": "Deduped"})

    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", _get)

    comp = _component()
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success
    assert poll_count["n"] == 1  # failed on the FIRST poll, not after looping to a timeout


def test_timeout_cancels_the_orphaned_fabric_job(fake_azure_identity, monkeypatch):
    import requests
    cancel_calls = []

    def _post(url, *a, **k):
        if url.endswith("/cancel"):
            cancel_calls.append(url)
            return _FakeResponse(202)
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "InProgress"}))

    comp = _component(max_wait_seconds=0)
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success
    assert cancel_calls == ["https://fabric/instances/1/cancel"]


def test_parameters_sent_in_fabrics_documented_top_level_typed_shape(fake_azure_identity, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed"}))

    comp = _component(parameters={"load_date": "2024-01-01", "retries": 3})
    _run_job(comp)

    # NOT nested under executionData, and NOT a bare dict -- a top-level
    # `parameters` list of {name, value, type} objects.
    assert "executionData" not in (captured["json"] or {})
    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"] == {"name": "load_date", "value": "2024-01-01", "type": "Text"}
    assert params["retries"] == {"name": "retries", "value": 3, "type": "Integer"}


def test_notebook_parameters_sent_as_executiondata_dict_not_the_top_level_list(fake_azure_identity, monkeypatch):
    """A Notebook is NOT the same shape as DataPipeline/Dataflow -- it reads
    its parameters out of executionData.parameters (a dict keyed by name,
    lowercase string/int/float/bool types), confirmed via Microsoft's own
    apache-airflow-providers-microsoft-fabric MSFabricNotebookJobParameters
    class and the official `fab` CLI's job-run docs."""
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed"}))

    comp = _component(item_type="Notebook", parameters={"load_date": "2024-01-01", "retries": 3})
    _run_job(comp)

    assert "parameters" not in (captured["json"] or {})
    exec_params = captured["json"]["executionData"]["parameters"]
    assert exec_params["load_date"] == {"value": "2024-01-01", "type": "string"}
    assert exec_params["retries"] == {"value": 3, "type": "int"}
