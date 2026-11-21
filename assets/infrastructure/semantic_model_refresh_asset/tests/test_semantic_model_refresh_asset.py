"""Real, committed tests for SemanticModelRefreshAssetComponent, the new component built against
the ADF-converter feedback's ask 5 (no Dagster-component equivalent of ADF's PBISemanticModelRefresh
activity existed). Mocks ONLY the Azure SDK/HTTP boundary (azure-identity isn't installed in this
environment at all -- faked via sys.modules, same pattern as fabric_pipeline_trigger_job's own
tests -- and `requests` is faked with canned responses); every other behavior (enhanced-vs-basic
refresh body construction, terminal-status detection, timeout, run_window skip) runs for real.
"""
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
SemanticModelRefreshAssetComponent = mod.SemanticModelRefreshAssetComponent


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
    identity_mod.ClientSecretCredential = _FakeCredential
    identity_mod.DefaultAzureCredential = _FakeCredential
    azure_mod.identity = identity_mod
    sys.modules["azure"] = azure_mod
    sys.modules["azure.identity"] = identity_mod
    yield
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def _component(**overrides):
    attrs = dict(asset_key="fabric/sales_model", workspace_id="ws-1", dataset_id="ds-1")
    attrs.update(overrides)
    return SemanticModelRefreshAssetComponent(**attrs)


def _asset_def(component):
    return component.build_defs(context=None).assets[0]


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


# ── refresh-body construction (pure logic, no mocking needed) ───────────────────────────────────

def test_no_fields_set_sends_no_body():
    assert _component()._refresh_body() is None


def test_notify_option_alone_is_a_basic_refresh():
    c = _component(notify_option="MailOnFailure")
    assert c._is_enhanced_refresh() is False
    assert c._refresh_body() == {"notifyOption": "MailOnFailure"}


def test_any_enhanced_field_triggers_enhanced_refresh_and_excludes_notify_option():
    c = _component(refresh_type="Full", commit_mode="transactional")
    assert c._is_enhanced_refresh() is True
    body = c._refresh_body()
    assert body == {"type": "Full", "commitMode": "transactional"}
    assert "notifyOption" not in body


def test_objects_list_is_passed_through_for_selective_refresh():
    c = _component(objects=[{"table": "Customer", "partition": "Robert"}])
    assert c._refresh_body() == {"objects": [{"table": "Customer", "partition": "Robert"}]}


def test_notify_option_and_enhanced_field_together_raises():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _component(notify_option="MailOnFailure", refresh_type="Full")


# ── dry_run ──────────────────────────────────────────────────────────────────────────────────────

def test_dry_run_skips_auth_and_the_real_rest_call():
    component = _component(dry_run=True, refresh_type="Full")
    asset_def = _asset_def(component)
    result = dg.materialize([asset_def], raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    assert events[0].materialization.metadata["status"].value == "dry_run — not executed"


# ── real trigger + poll, against a faked requests/azure.identity boundary ──────────────────────────

def test_triggers_and_polls_to_completed(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-1",
                      "x-ms-request-id": "r-1"},
    ))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Completed", "startTime": "2024-01-01T00:00:00", "endTime": "2024-01-01T00:05:00",
        "numberOfAttempts": 1,
    }))

    component = _component(poll_interval_seconds=0)
    result = dg.materialize([_asset_def(component)], raise_on_error=True)
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["powerbi/status"].value == "Completed"
    assert md["powerbi/request_id"].value == "r-1"


def test_raises_on_failed_status_with_message(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-2"},
    ))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {
        "status": "Failed", "messages": [{"message": "boom", "type": "Error"}],
    }))

    component = _component(poll_interval_seconds=0)
    with pytest.raises(Exception, match="boom"):
        dg.materialize([_asset_def(component)], raise_on_error=True)


def test_raises_on_disabled_status(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-3"},
    ))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"status": "Disabled"}))

    component = _component(poll_interval_seconds=0)
    with pytest.raises(Exception, match="Disabled"):
        dg.materialize([_asset_def(component)], raise_on_error=True)


def test_in_progress_keeps_polling_until_terminal(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-4"},
    ))
    responses = iter([
        _FakeResponse(202, {"status": "Unknown"}),
        _FakeResponse(202, {"status": "Unknown"}),
        _FakeResponse(200, {"status": "Completed"}),
    ])
    monkeypatch.setattr(requests, "get", lambda *a, **k: next(responses))

    component = _component(poll_interval_seconds=0)
    result = dg.materialize([_asset_def(component)], raise_on_error=True)
    assert result.success


def test_timeout_raises_after_max_wait_seconds(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-5"},
    ))
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(202, {"status": "Unknown"}))

    component = _component(poll_interval_seconds=0, max_wait_seconds=0)
    with pytest.raises(Exception, match="timed out"):
        dg.materialize([_asset_def(component)], raise_on_error=True)


def test_trigger_failure_raises_a_clear_error(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(403, text="forbidden"))
    component = _component()
    with pytest.raises(Exception, match="403"):
        dg.materialize([_asset_def(component)], raise_on_error=True)


def test_wait_for_completion_false_returns_without_polling(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: _FakeResponse(
        202, headers={"Location": "https://api.powerbi.com/v1.0/myorg/groups/ws-1/datasets/ds-1/refreshes/r-6"},
    ))
    monkeypatch.setattr(requests, "get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not poll")))

    component = _component(wait_for_completion=False)
    result = dg.materialize([_asset_def(component)], raise_on_error=True)
    assert result.success


# ── run_window ───────────────────────────────────────────────────────────────────────────────────

def test_run_window_outside_skips_without_calling_fabric(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not trigger outside run_window")))

    component = _component(run_window="00:00-00:01 UTC")  # a window almost certainly not "now"
    import time as _time
    # force a time clearly outside the 1-minute window
    monkeypatch.setattr(mod.time, "time", lambda: 1735707600.0)  # 2025-01-01 01:00:00 UTC
    result = dg.materialize([_asset_def(component)], raise_on_error=True)
    assert result.success
