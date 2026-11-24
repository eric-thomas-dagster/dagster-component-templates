"""Real tests for RunMyJobsIntegrationComponent's observe-only mode.

Uses real dg.build_sensor_context()/dg.Definitions -- the only thing mocked is the
unavoidable `requests.get` call the live-mode sensor branch makes against the RMJ REST API.
"""
import json
import types

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _run_sensor(sensor, ctx):
    result = sensor(ctx)
    if isinstance(result, types.GeneratorType):
        result = list(result)
    return result


def test_observed_chains_build_non_executable_external_assets(mod):
    comp = mod.RunMyJobsIntegrationComponent(
        demo_mode=True,
        jobs=[],
        observed_chains=[
            mod.RunMyJobsObservedChainSpec(job_definition="EOD_BATCH_SETTLEMENT", asset_name="rmj_eod_settlement"),
        ],
    )
    defs = comp.build_defs(None)
    specs = {a.key: a for a in defs.assets}
    assert dg.AssetKey(["rmj_eod_settlement"]) in specs
    spec = specs[dg.AssetKey(["rmj_eod_settlement"])]
    # A plain AssetSpec has no op/compute body -- it is observe-only by construction.
    assert isinstance(spec, dg.AssetSpec)
    assert spec.metadata["dagster.observability_type"] == "external"
    assert spec.partitions_def is not None


def test_jobs_field_still_builds_executable_assets(mod):
    """jobs: (Dagster-submits) and observed_chains: (RMJ-submits) coexist."""
    comp = mod.RunMyJobsIntegrationComponent(
        demo_mode=True,
        jobs=[mod.RunMyJobsJobSpec(job_definition="X", asset_name="x_asset")],
        observed_chains=[mod.RunMyJobsObservedChainSpec(job_definition="Y", asset_name="y_asset")],
    )
    defs = comp.build_defs(None)
    keys = {a.key for a in defs.assets}
    assert dg.AssetKey(["x_asset"]) in keys
    assert dg.AssetKey(["y_asset"]) in keys
    result = dg.materialize(
        [a for a in defs.assets if isinstance(a, dg.AssetsDefinition) and a.key == dg.AssetKey(["x_asset"])],
        partition_key="2024-01-01",
    )
    assert result.success


def test_demo_monitor_is_deterministic_and_per_chain(mod):
    """Fixes: demo mode used to emit a random single observation on jobs[0] every tick."""
    comp = mod.RunMyJobsIntegrationComponent(
        demo_mode=True,
        jobs=[],
        observed_chains=[
            mod.RunMyJobsObservedChainSpec(job_definition="EOD_BATCH_SETTLEMENT", asset_name="rmj_eod_settlement"),
            mod.RunMyJobsObservedChainSpec(job_definition="REGULATORY_EXTRACT", asset_name="rmj_regulatory_extract"),
        ],
    )
    defs = comp.build_defs(None)
    sensor = defs.get_sensor_def("runmyjobs_external_execution_monitor")

    ctx = dg.build_sensor_context()
    result = _run_sensor(sensor, ctx)
    assert len(result) == 1
    events = result[0].asset_events
    emitted_keys = {e.asset_key for e in events}
    # Both declared chains get a materialization -- not just jobs[0].
    assert emitted_keys == {dg.AssetKey(["rmj_eod_settlement"]), dg.AssetKey(["rmj_regulatory_extract"])}
    for e in events:
        assert isinstance(e, dg.AssetMaterialization)
        assert e.partition is not None  # business-date-derived partition

    # Running again with the SAME day's cursor must not replay the same completions.
    ctx2 = dg.build_sensor_context(cursor=result[0].cursor)
    result2 = _run_sensor(sensor, ctx2)
    # No new events: either an empty result list, or a result with no asset_events.
    total_events = sum(len(r.asset_events) for r in result2) if result2 else 0
    assert total_events == 0


def test_no_observed_chains_is_a_safe_noop(mod):
    comp = mod.RunMyJobsIntegrationComponent(demo_mode=True, jobs=[mod.RunMyJobsJobSpec(job_definition="X", asset_name="x")])
    defs = comp.build_defs(None)
    sensor = defs.get_sensor_def("runmyjobs_external_execution_monitor")
    ctx = dg.build_sensor_context()
    result = _run_sensor(sensor, ctx)
    assert result == [] or all(not r.asset_events for r in result)


def test_live_mode_maps_job_definition_to_declared_asset_and_partition(mod, monkeypatch):
    comp = mod.RunMyJobsIntegrationComponent(
        demo_mode=False,
        jobs=[],
        observed_chains=[
            mod.RunMyJobsObservedChainSpec(job_definition="EOD_BATCH_SETTLEMENT", asset_name="rmj_eod_settlement"),
        ],
        business_date_parameter="BUSINESS_DATE",
    )
    monkeypatch.setenv("RUNMYJOBS_USER", "svc")
    monkeypatch.setenv("RUNMYJOBS_PASSWORD", "pw")

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "processes": [
                    {
                        "jobDefinition": "EOD_BATCH_SETTLEMENT",
                        "processId": "RMJ-ABC123",
                        "status": "Completed",
                        "runEnd": "2024-06-01T02:00:00Z",
                        "parameters": {"BUSINESS_DATE": "2024-06-01"},
                    },
                    {
                        # Untracked jobDefinition -- must be skipped, not crash.
                        "jobDefinition": "SOME_OTHER_CHAIN",
                        "processId": "RMJ-XYZ",
                        "status": "Completed",
                        "runEnd": "2024-06-01T02:05:00Z",
                        "parameters": {},
                    },
                ]
            }

    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Resp()

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    defs = comp.build_defs(None)
    sensor = defs.get_sensor_def("runmyjobs_external_execution_monitor")
    ctx = dg.build_sensor_context()
    result = _run_sensor(sensor, ctx)

    assert len(result) == 1
    events = result[0].asset_events
    assert len(events) == 1
    event = events[0]
    assert event.asset_key == dg.AssetKey(["rmj_eod_settlement"])
    assert event.partition == "2024-06-01"
    assert "/scheduler/api/processes" in calls[0][0]
    assert calls[0][1]["params"]["status"] == "Completed"

    # Cursor dedups the same processId on a second poll.
    cursor_state = json.loads(result[0].cursor)
    assert "RMJ-ABC123" in cursor_state["seen_process_ids"]
    ctx2 = dg.build_sensor_context(cursor=result[0].cursor)
    result2 = _run_sensor(sensor, ctx2)
    total_events = sum(len(r.asset_events) for r in result2) if result2 else 0
    assert total_events == 0


def test_include_operations_gates_restart_hold_kill_jobs(mod):
    comp_off = mod.RunMyJobsIntegrationComponent(demo_mode=True, jobs=[mod.RunMyJobsJobSpec(job_definition="X", asset_name="x")])
    defs_off = comp_off.build_defs(None)
    assert list(defs_off.jobs) == []
    assert list(defs_off.schedules) == []

    comp_on = mod.RunMyJobsIntegrationComponent(
        demo_mode=True, include_operations=True, jobs=[mod.RunMyJobsJobSpec(job_definition="X", asset_name="x")],
    )
    defs_on = comp_on.build_defs(None)
    job_names = {j.name for j in defs_on.jobs}
    assert job_names == {
        "runmyjobs_restart_process", "runmyjobs_hold_application",
        "runmyjobs_release_processes", "runmyjobs_kill_process",
        "runmyjobs_reconciliation",
    }
    assert [s.name for s in defs_on.schedules] == ["runmyjobs_reconciliation_schedule"]
