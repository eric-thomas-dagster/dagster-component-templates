"""Real tests for ArgoWorkflowTriggerComponent's partitions/substitution/wait_for_completion/demo_mode.

Real dg.materialize() throughout (demo_mode=True needs no network). The one
non-demo test mocks only the unavoidable `requests` calls against the Argo Server.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_demo_mode_materializes_with_no_network(mod):
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
        parameters={"INPUT_DATE": "2026-05-27"},
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["demo_mode"].value is True
    assert md["argo_workflow_template"].text == "nightly-aggregation"
    assert "argo_workflow_name" in md


def test_partition_key_substitution_in_parameters(mod):
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
        parameters={"INPUT_DATE": "{partition_key}", "LITERAL": "{date}-suffix"},
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), partition_key="2024-06-01")
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    rendered = md["argo_parameters"].data
    assert rendered["INPUT_DATE"] == "2024-06-01"
    assert rendered["LITERAL"] == "2024-06-01-suffix"


def test_wait_for_completion_surfaces_outputs_in_demo_mode(mod):
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
        parameters={"REGION": "us-east-1"},
        wait_for_completion=True,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["argo_phase"].text == "Succeeded"
    assert "argo_outputs" in md


def test_no_partitions_def_when_unset(mod):
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
    )
    defs = comp.build_defs(None)
    asset_def = next(iter(defs.assets))
    assert asset_def.partitions_def is None


def test_live_mode_fails_asset_on_argo_failed_phase(mod, monkeypatch):
    """wait_for_completion=true + demo_mode=false must raise, not silently succeed, on Failed."""
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
        demo_mode=False,
        wait_for_completion=True,
        poll_interval_seconds=1,
    )
    monkeypatch.setenv("ARGO_SERVER_URL", "https://argo.example.com")
    monkeypatch.delenv("ARGO_TOKEN", raising=False)

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    def fake_post(url, **kwargs):
        assert url == "https://argo.example.com/api/v1/workflows/argo/submit"
        return _Resp({"metadata": {"name": "nightly-aggregation-xyz12"}})

    def fake_get(url, **kwargs):
        assert url == "https://argo.example.com/api/v1/workflows/argo/nightly-aggregation-xyz12"
        return _Resp({"status": {"phase": "Failed", "message": "boom"}})

    import requests
    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_live_mode_submits_and_outputs_on_success(mod, monkeypatch):
    comp = mod.ArgoWorkflowTriggerComponent(
        asset_key="argo/etl/nightly_aggregation",
        workflow_template="nightly-aggregation",
        parameters={"REGION": "{partition_key}"},
        partition_type="daily",
        partition_start="2024-01-01",
        demo_mode=False,
        wait_for_completion=True,
        poll_interval_seconds=1,
    )
    monkeypatch.setenv("ARGO_SERVER_URL", "https://argo.example.com")
    monkeypatch.delenv("ARGO_TOKEN", raising=False)

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    posted = {}

    def fake_post(url, **kwargs):
        posted["json"] = kwargs["json"]
        return _Resp({"metadata": {"name": "nightly-aggregation-xyz12"}})

    def fake_get(url, **kwargs):
        return _Resp({
            "status": {
                "phase": "Succeeded",
                "outputs": {"parameters": [{"name": "result", "value": "42"}]},
            }
        })

    import requests
    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), partition_key="2024-06-01")
    assert result.success
    assert posted["json"]["submitOptions"]["parameters"] == ["REGION=2024-06-01"]
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["argo_phase"].text == "Succeeded"
    assert md["argo_outputs"].data == [{"name": "result", "value": "42"}]
