"""Real, committed tests for gaps a demo-builder reported against
`_trigger_item_run` (dagster-demo-factory component-feedback, fabric_workspace
v1.1.0):

1. A failed/cancelled/timed-out/deduped Fabric run previously RETURNED the
   state dict instead of raising -- a green Dagster materialization on bad
   data. Now raises `dg.Failure` (or `dg.RetryRequested` when Fabric's own
   `failureReason.isRetriable` flag says the failure is transient).
2. `Deduped` wasn't in the terminal-status set, so a collision just polled
   until the timeout instead of failing immediately with a clear message.
3. No parameters were ever sent -- a notebook couldn't receive a load date
   or any per-run override. Fixed via `default_parameters`/`item_parameters`,
   converted to Fabric's documented `Parameter[]` request shape
   ({name, value, type} -- verified the SAME shape is used across item
   types, not a different shape per item type as sometimes assumed).
4. Auth failures escaped as a raw azure.identity exception instead of a
   clear Dagster Failure.
5. Timeout now requests cancellation of the orphaned Fabric job instead of
   leaving it running; a job that reached its own terminal state on Fabric's
   side (Completed/Failed/Cancelled/Deduped) does NOT get a pointless
   cancel call.

All HTTP calls are faked at the `requests` module boundary (not mocked
business logic) -- this is a real local function exercising the real
polling loop against scripted fake responses.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("fabric_workspace_trigger", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent
FabricResource = _mod.FabricResource


def _component(**overrides):
    attrs = dict(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=True,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


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


@pytest.fixture
def auth(monkeypatch):
    monkeypatch.setattr(FabricResource, "get_token", lambda self: "fake-token")


def _ctx():
    return dg.build_asset_context()


def test_completed_job_returns_the_state_dict(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    state = comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert state["status"] == "Completed"


def test_failed_job_raises_failure_not_returns(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "NotebookExecutionFailed", "message": "cell 3 raised", "isRetriable": False},
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "NotebookExecutionFailed" in str(exc_info.value.metadata["fabric_error_code"].value)
    assert "cell 3 raised" in exc_info.value.description


def test_retriable_failure_raises_retry_requested(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "id": "job-1",
        "failureReason": {"errorCode": "CapacityThrottled", "message": "capacity paused", "isRetriable": True},
    }))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.RetryRequested) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert isinstance(exc_info.value.__cause__, dg.Failure)


def test_cancelled_job_raises_failure(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Cancelled", "id": "job-1", "failureReason": None}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure):
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")


def test_deduped_job_raises_a_clear_collision_failure_not_a_timeout(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Deduped", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "Deduped" in exc_info.value.description


def test_timeout_raises_failure_and_cancels_the_job(auth, monkeypatch):
    import requests
    cancel_calls = []
    monkeypatch.setattr(requests, "post", lambda url, *a, **k: (
        cancel_calls.append(url) or _FakeResponse(202)
    ) if url.endswith("/cancel") else _FakeResponse(202, headers={"Location": "https://fabric/instances/1"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "InProgress", "id": "job-1"}))

    comp = _component(workspace=FabricResource(
        workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s",
        poll_interval_seconds=0, max_wait_seconds=0,
    ))
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "timed out" in exc_info.value.description
    assert cancel_calls == ["https://fabric/instances/1/cancel"]


def test_completed_job_does_not_call_cancel(auth, monkeypatch):
    import requests
    cancel_calls = []

    def _post(url, *a, **k):
        if url.endswith("/cancel"):
            cancel_calls.append(url)
            return _FakeResponse(202)
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert cancel_calls == []


def test_auth_failure_raises_a_clear_dagster_failure_not_a_raw_exception(monkeypatch):
    def _raise(self):
        raise RuntimeError("azure.identity: no credentials found")
    monkeypatch.setattr(FabricResource, "get_token", _raise)

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "DataPipeline")
    assert "authentication failed" in exc_info.value.description.lower()


def test_trigger_failure_response_parses_the_real_fabric_error_shape(auth, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        403, json_body={"errorCode": "InsufficientPrivileges", "message": "caller lacks Notebook.Execute.All"},
    ))

    comp = _component()
    with pytest.raises(dg.Failure) as exc_info:
        comp._trigger_item_run(_ctx(), "item-1", "Notebook")
    assert "InsufficientPrivileges" in exc_info.value.metadata["fabric_error_code"].value
    assert "caller lacks" in exc_info.value.description


def test_parameters_are_sent_in_fabrics_documented_shape(auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    comp._trigger_item_run(_ctx(), "item-1", "Notebook", parameters={"load_date": "2024-01-01", "retries": 3, "dry_run": True})

    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"] == {"name": "load_date", "value": "2024-01-01", "type": "Text"}
    assert params["retries"] == {"name": "retries", "value": 3, "type": "Integer"}
    assert params["dry_run"] == {"name": "dry_run", "value": True, "type": "Boolean"}


def test_no_parameters_sends_no_request_body(auth, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return _FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Completed", "id": "job-1"}))

    comp = _component(workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s", poll_interval_seconds=0))
    comp._trigger_item_run(_ctx(), "item-1", "Notebook")
    assert captured["json"] is None
