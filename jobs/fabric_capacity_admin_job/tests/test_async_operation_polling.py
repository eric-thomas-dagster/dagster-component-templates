"""Real, committed tests for a gap found while comparing fabric-family
components against Microsoft's own Fabric Capacities REST API (ARM):
`suspend`/`resume`/`scale` return 202 Accepted with an `Azure-AsyncOperation`
header (confirmed via learn.microsoft.com/rest/api/microsoftfabric/
fabric-capacities/suspend's own documented example, which shows BOTH
`Azure-AsyncOperation` and `Location` on its sample 202 response) -- this
component logged that header and considered the job done immediately,
without ever polling it. For `suspend` specifically, that means a nightly
pause schedule could report success in Dagster well before the capacity
has actually stopped billing -- the entire point of pausing on a schedule.

Fixed via `_wait_for_arm_operation`, the standard ARM long-running-operation
polling contract (prefer Azure-AsyncOperation over Location; GET it; a
`status` field of Succeeded/Failed/Canceled is terminal; use Retry-After for
poll cadence).
"""
import dagster as dg
import pytest

from .conftest import FakeResponse, load_component_module

mod = load_component_module()
FabricCapacityAdminJobComponent = mod.FabricCapacityAdminJobComponent


def _component(**overrides):
    attrs = dict(
        job_name="fabric_pause", action="suspend",
        subscription_id="sub-1", resource_group_name="rg-1", capacity_name="cap1",
        poll_interval_seconds=0,
    )
    attrs.update(overrides)
    return FabricCapacityAdminJobComponent(**attrs)


def _run(comp):
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    return job.execute_in_process(raise_on_error=True)


def test_suspend_waits_for_the_async_operation_to_succeed(fake_azure_identity, monkeypatch):
    import requests
    poll_count = {"n": 0}

    def _post(url, **k):
        return FakeResponse(202, headers={
            "azure-asyncoperation": "https://management.azure.com/async-op-1",
            "Location": "https://management.azure.com/location-1",
        })

    def _get(url, **k):
        poll_count["n"] += 1
        assert url == "https://management.azure.com/async-op-1"  # prefers Azure-AsyncOperation over Location
        if poll_count["n"] < 2:
            return FakeResponse(200, {"status": "Running"})
        return FakeResponse(200, {"status": "Succeeded"})

    monkeypatch.setattr(requests, "post", _post)
    monkeypatch.setattr(requests, "get", _get)

    comp = _component()
    result = _run(comp)
    assert result.success
    assert poll_count["n"] == 2  # polled until terminal, not just accepted the 202


def test_failed_async_operation_raises_a_real_failure(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse(202, headers={"azure-asyncoperation": "https://mgmt/op"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"status": "Failed", "error": {"message": "capacity in use"}}))

    comp = _component()
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success


def test_wait_for_completion_false_skips_polling_entirely(fake_azure_identity, monkeypatch):
    import requests
    get_calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse(202, headers={"azure-asyncoperation": "https://mgmt/op"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: get_calls.append(1))

    comp = _component(wait_for_completion=False)
    result = _run(comp)
    assert result.success
    assert get_calls == []  # never polled


def test_no_async_operation_header_is_treated_as_already_done(fake_azure_identity, monkeypatch):
    import requests
    get_calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse(200))  # synchronous completion, no headers
    monkeypatch.setattr(requests, "get", lambda *a, **k: get_calls.append(1))

    comp = _component()
    result = _run(comp)
    assert result.success
    assert get_calls == []


def test_timeout_raises_a_clear_failure(fake_azure_identity, monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse(202, headers={"azure-asyncoperation": "https://mgmt/op"}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"status": "Running"}))

    comp = _component(max_wait_seconds=0)
    defs = comp.build_defs(None)
    job = next(iter(defs.jobs))
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success
