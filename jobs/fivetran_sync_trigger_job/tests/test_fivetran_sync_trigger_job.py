"""Real, committed tests for FivetranSyncTriggerJobComponent.

Covers the new `wait_for_completion` polling (Fivetran has no discrete
sync/job id, so completion is detected via `succeeded_at`/`failed_at`
baseline-comparison, per Fivetran's own documented pattern:
fivetran.com/docs/rest-api/how-to/run-manual-sync-and-poll-for-completion)
and the asset-shape addition (migration-skills audit: "asset form that
waits and returns the job id"). Mocks only the actual Fivetran REST API
boundary (`requests`).
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
    monkeypatch.setenv("FIVETRAN_API_KEY", "k")
    monkeypatch.setenv("FIVETRAN_API_SECRET", "s")


def _resp(json_body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    r.text = str(json_body)
    return r


def test_requires_job_name_or_asset_name(mod):
    with pytest.raises(Exception):
        mod.FivetranSyncTriggerJobComponent(connector_id="c1")


@patch("requests.post")
def test_asset_shape_fire_and_forget(mock_post, mod):
    mock_post.return_value = _resp({})
    component = mod.FivetranSyncTriggerJobComponent(
        asset_name="raw/orders_sync", connector_id="c1", deps=["upstream/trigger_signal"],
    )
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    spec = next(iter(asset_defs[0].specs))
    assert spec.key == dg.AssetKey(["raw", "orders_sync"])
    assert dg.AssetKey(["upstream", "trigger_signal"]) in {d.asset_key for d in spec.deps}

    result = dg.materialize(asset_defs)
    assert result.success
    mats = result.asset_materializations_for_node(asset_defs[0].node_def.name)
    assert mats[0].metadata["fivetran/connector_id"].value == "c1"
    assert mock_post.call_args.args[0].endswith("/connectors/c1/force")


@patch("requests.post")
@patch("requests.get")
def test_asset_shape_waits_via_timestamp_baseline(mock_get, mock_post, mod):
    mock_get.side_effect = [
        _resp({"data": {"succeeded_at": "2026-01-01T00:00:00Z", "failed_at": None}}),  # baseline
        _resp({"data": {"succeeded_at": "2026-01-01T00:00:00Z", "failed_at": None, "status": {"sync_state": "syncing"}}}),
        _resp({"data": {"succeeded_at": "2026-01-01T01:00:00Z", "failed_at": None, "status": {"sync_state": "scheduled"}}}),
    ]
    mock_post.return_value = _resp({})
    component = mod.FivetranSyncTriggerJobComponent(
        asset_name="raw/x", connector_id="c1", wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = result.asset_materializations_for_node(list(defs.assets)[0].node_def.name)
    assert mats[0].metadata["fivetran/status"].value == "succeeded"


@patch("requests.post")
@patch("requests.get")
def test_asset_shape_raises_on_failed_at_change(mock_get, mock_post, mod):
    mock_get.side_effect = [
        _resp({"data": {"succeeded_at": None, "failed_at": None}}),  # baseline
        _resp({"data": {"succeeded_at": None, "failed_at": "2026-01-01T00:05:00Z", "status": {"sync_state": "scheduled"}}}),
    ]
    mock_post.return_value = _resp({})
    component = mod.FivetranSyncTriggerJobComponent(
        asset_name="raw/x", connector_id="c1", wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


@patch("requests.post")
def test_job_shape_still_works(mock_post, mod):
    mock_post.return_value = _resp({})
    component = mod.FivetranSyncTriggerJobComponent(job_name="sync_c1", connector_id="c1")
    defs = component.build_defs(context=None)
    result = defs.get_job_def("sync_c1").execute_in_process()
    assert result.success
