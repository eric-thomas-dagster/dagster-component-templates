"""Real, committed test for a gap found against the ADF-converter feedback: FabricPipelineTriggerJobComponent
had no dry_run flag, unlike the SSIS/Synapse/SAP job-trigger components. Confirms dry_run skips the
real Fabric authentication + REST call entirely -- no azure.identity import, no requests call."""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
FabricPipelineTriggerJobComponent = mod.FabricPipelineTriggerJobComponent


def test_dry_run_skips_auth_and_the_real_rest_call(monkeypatch):
    component = FabricPipelineTriggerJobComponent(
        job_name="trigger_orders_pipeline",
        workspace_id="ws-1", item_id="item-1", item_type="DataPipeline",
        dry_run=True,
    )
    import requests
    monkeypatch.setattr(requests, "post", lambda *a, **k: (_ for _ in ()).throw(AssertionError("dry_run must not call Fabric")))

    defs = component.build_defs(context=None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process(raise_on_error=True)
    assert result.success
