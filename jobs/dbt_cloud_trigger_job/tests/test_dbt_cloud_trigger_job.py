"""Real, committed tests for DbtCloudTriggerJobComponent.

Covers the asset-shape addition (migration-skills audit: "asset form with
deps") alongside the pre-existing op-job shape, using a fake
`DbtCloudResource` (mocking only the dbt Cloud client boundary, never dbt
Cloud itself).
"""
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


class FakeDbtCloudResource:
    def __init__(self, run_id=111, status="Success", should_fail=False):
        self.run_id = run_id
        self.status = status
        self.should_fail = should_fail
        self.run_job_and_poll_calls = []
        self.run_job_calls = []

    def run_job_and_poll(self, job_id, poll_interval, poll_timeout, **kwargs):
        self.run_job_and_poll_calls.append({"job_id": job_id, **kwargs})
        if self.should_fail:
            raise Exception("dbt Cloud run failed")
        return SimpleNamespace(
            run_details={"id": self.run_id, "href": "https://cloud.getdbt.com/run/111", "status_humanized": self.status},
            result=None,
        )

    def run_job(self, job_id, **kwargs):
        self.run_job_calls.append({"job_id": job_id, **kwargs})
        return {"id": self.run_id}

    def build_url_for_job(self, job_id):
        return f"https://cloud.getdbt.com/job/{job_id}"


def test_requires_job_name_or_asset_name(mod):
    with pytest.raises(Exception):
        mod.DbtCloudTriggerJobComponent(dbt_cloud_job_id=1)


def test_job_shape_still_works(mod):
    component = mod.DbtCloudTriggerJobComponent(job_name="trigger_dbt", dbt_cloud_job_id=67890)
    defs = component.build_defs(context=None)
    resource = FakeDbtCloudResource()
    full_defs = dg.Definitions(jobs=defs.jobs, resources={"dbt_cloud_resource": resource})
    result = full_defs.get_job_def("trigger_dbt").execute_in_process()
    assert result.success
    assert resource.run_job_and_poll_calls[0]["job_id"] == 67890


def test_asset_shape_materializes_with_deps(mod):
    component = mod.DbtCloudTriggerJobComponent(
        asset_name="mart/daily_summary", dbt_cloud_job_id=67890, deps=["raw/snowpipe_landing"],
    )
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    assert len(asset_defs) == 1
    spec = next(iter(asset_defs[0].specs))
    assert spec.key == dg.AssetKey(["mart", "daily_summary"])
    assert dg.AssetKey(["raw", "snowpipe_landing"]) in {d.asset_key for d in spec.deps}

    resource = FakeDbtCloudResource(run_id=222)
    result = dg.materialize(asset_defs, resources={"dbt_cloud_resource": resource})
    assert result.success
    mats = result.asset_materializations_for_node(asset_defs[0].node_def.name)
    assert mats[0].metadata["dbt_cloud/run_id"].value == 222
    assert resource.run_job_and_poll_calls[0]["job_id"] == 67890


def test_asset_shape_forwards_cause_and_overrides(mod):
    component = mod.DbtCloudTriggerJobComponent(
        asset_name="mart/x", dbt_cloud_job_id=1, cause="triggered on snowpipe load",
        steps_override=["dbt build --select tag:hourly"], schema_override="branch_123",
        git_branch="feature/x", git_sha="abc123",
    )
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    resource = FakeDbtCloudResource()
    dg.materialize(asset_defs, resources={"dbt_cloud_resource": resource})
    call = resource.run_job_and_poll_calls[0]
    assert call["cause"] == "triggered on snowpipe load"
    assert call["steps_override"] == ["dbt build --select tag:hourly"]
    assert call["schema_override"] == "branch_123"
    assert call["git_branch"] == "feature/x"
    assert call["git_sha"] == "abc123"


def test_asset_shape_raises_on_dbt_cloud_failure(mod):
    component = mod.DbtCloudTriggerJobComponent(asset_name="mart/x", dbt_cloud_job_id=1)
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    resource = FakeDbtCloudResource(should_fail=True)
    result = dg.materialize(asset_defs, resources={"dbt_cloud_resource": resource}, raise_on_error=False)
    assert not result.success
