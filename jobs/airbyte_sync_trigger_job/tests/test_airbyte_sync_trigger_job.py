"""Real, committed tests for AirbyteSyncTriggerJobComponent.

Covers the asset-shape addition (migration-skills audit: "asset form that
waits and returns the job id") alongside the pre-existing op-job shape,
mocking only the actual Airbyte REST API boundary (`requests`).
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AIRBYTE_USER", "u")
    monkeypatch.setenv("AIRBYTE_PASSWORD", "p")


def _resp(json_body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    r.text = str(json_body)
    return r


def test_requires_job_name_or_asset_name(mod):
    with pytest.raises(Exception):
        mod.AirbyteSyncTriggerJobComponent(airbyte_url="https://x", connection_id="c1")


@patch("requests.post")
def test_asset_shape_fire_and_forget_returns_job_id(mock_post, mod):
    mock_post.return_value = _resp({"job": {"id": 42}})
    component = mod.AirbyteSyncTriggerJobComponent(
        asset_name="raw/orders_sync", airbyte_url="https://airbyte.acme.com", connection_id="c1",
        deps=["upstream/trigger_signal"],
    )
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    spec = next(iter(asset_defs[0].specs))
    assert spec.key == dg.AssetKey(["raw", "orders_sync"])
    assert dg.AssetKey(["upstream", "trigger_signal"]) in {d.asset_key for d in spec.deps}

    result = dg.materialize(asset_defs)
    assert result.success
    mats = result.asset_materializations_for_node(asset_defs[0].node_def.name)
    assert mats[0].metadata["airbyte/job_id"].value == 42


@patch("requests.post")
def test_asset_shape_waits_for_completion(mock_post, mod):
    mock_post.side_effect = [
        _resp({"job": {"id": 7}}),
        _resp({"job": {"status": "running"}}),
        _resp({"job": {"status": "succeeded"}}),
    ]
    component = mod.AirbyteSyncTriggerJobComponent(
        asset_name="raw/orders_sync", airbyte_url="https://airbyte.acme.com", connection_id="c1",
        wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success


@patch("requests.post")
def test_asset_shape_raises_on_failed_sync(mock_post, mod):
    mock_post.side_effect = [
        _resp({"job": {"id": 7}}),
        _resp({"job": {"status": "failed"}}),
    ]
    component = mod.AirbyteSyncTriggerJobComponent(
        asset_name="raw/x", airbyte_url="https://airbyte.acme.com", connection_id="c1",
        wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


@patch("requests.post")
def test_job_shape_still_works(mock_post, mod):
    mock_post.return_value = _resp({"job": {"id": 99}})
    component = mod.AirbyteSyncTriggerJobComponent(
        job_name="sync_orders", airbyte_url="https://airbyte.acme.com", connection_id="c1",
    )
    defs = component.build_defs(context=None)
    result = defs.get_job_def("sync_orders").execute_in_process()
    assert result.success
