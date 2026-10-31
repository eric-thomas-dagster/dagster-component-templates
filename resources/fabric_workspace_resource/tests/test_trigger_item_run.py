"""Real, committed tests for gaps found in FabricWorkspaceResource.trigger_item_run
while comparing fabric-family components against Microsoft's own
apache-airflow-providers-microsoft-fabric (a separate, standalone duplicate
of the trigger/poll logic in `fabric_workspace`/`fabric_pipeline_trigger_job`
that had never been touched by the fixes applied to those two):

1. `parameters` were ALWAYS sent as `{"executionData": {"parameters": <raw
   dict>}}` regardless of item type -- wrong for DataPipeline/Dataflow
   (should be the top-level typed `parameters` list) and wrong even for
   Notebook (needed a typed {value, type} dict, not a bare value). Fixed
   with the same per-item-type branching as the other two components,
   confirmed via Microsoft's own MSFabricNotebookJobParameters dataclass and
   the official `fab` CLI's job-run docs.
2. `wait_for_run` never treated `Deduped` as terminal, so a scheduling
   collision polled until `max_wait_seconds` instead of returning
   immediately.
"""
import dagster as dg
import pytest

from .conftest import FakeResponse, load_component_module

mod = load_component_module()
FabricWorkspaceResource = mod.FabricWorkspaceResource


def _resource(**overrides):
    attrs = dict(workspace_id="ws-1", tenant_id_env_var=None, client_id_env_var=None, client_secret_env_var=None)
    attrs.update(overrides)
    return FabricWorkspaceResource(**attrs)


@pytest.fixture
def fake_token(monkeypatch):
    monkeypatch.setattr(FabricWorkspaceResource, "_get_token", lambda self: "fake-token")


def test_datapipeline_parameters_use_the_top_level_typed_list(fake_token, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    res = _resource()
    res.trigger_item_run("item-1", "DataPipeline", {"load_date": "2024-01-01", "retries": 3, "dry_run": True})

    assert "executionData" not in captured["json"]
    params = {p["name"]: p for p in captured["json"]["parameters"]}
    assert params["load_date"] == {"name": "load_date", "value": "2024-01-01", "type": "Text"}
    assert params["retries"] == {"name": "retries", "value": 3, "type": "Integer"}
    assert params["dry_run"] == {"name": "dry_run", "value": True, "type": "Boolean"}


def test_notebook_parameters_use_the_typed_executiondata_dict(fake_token, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    res = _resource()
    res.trigger_item_run("item-1", "Notebook", {"load_date": "2024-01-01", "retries": 3, "dry_run": True})

    assert "parameters" not in captured["json"]
    exec_params = captured["json"]["executionData"]["parameters"]
    assert exec_params["load_date"] == {"value": "2024-01-01", "type": "string"}
    assert exec_params["retries"] == {"value": 3, "type": "int"}
    assert exec_params["dry_run"] == {"value": True, "type": "bool"}


def test_no_parameters_sends_no_request_body(fake_token, monkeypatch):
    import requests
    captured = {}

    def _post(url, json=None, **k):
        captured["json"] = json
        return FakeResponse(202, headers={"Location": "https://fabric/instances/1"})

    monkeypatch.setattr(requests, "post", _post)
    res = _resource()
    res.trigger_item_run("item-1", "DataPipeline")
    assert captured["json"] is None


def test_deduped_run_returns_immediately_not_after_a_timeout_style_poll(fake_token, monkeypatch):
    import requests
    poll_count = {"n": 0}

    def _get(*a, **k):
        poll_count["n"] += 1
        return FakeResponse(200, {"status": "Deduped"})

    monkeypatch.setattr(requests, "get", _get)
    res = _resource()
    state = res.wait_for_run("https://fabric/instances/1", max_wait_seconds=9999, poll_interval=0)
    assert state["status"] == "Deduped"
    assert poll_count["n"] == 1
