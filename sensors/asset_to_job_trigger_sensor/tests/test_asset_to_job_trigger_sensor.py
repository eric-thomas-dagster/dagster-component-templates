import importlib.util
import pathlib

import dagster as dg
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "asset_to_job_trigger_sensor_component", here / "component.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
AssetToJobTriggerSensorComponent = mod.AssetToJobTriggerSensorComponent


def _component(**overrides):
    attrs = dict(
        sensor_name="test_trigger_sensor",
        monitored_selection=["upstream/a"],
        job_name="downstream_job",
    )
    attrs.update(overrides)
    return AssetToJobTriggerSensorComponent(**attrs)


def test_explicit_list_selection_builds_a_real_sensor():
    comp = _component()
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors
    assert sensor.name == "test_trigger_sensor"


def test_selection_dsl_resolves_against_real_sibling_assets():
    """Exercises the real AssetSelection.from_string() path (group:), not
    just the explicit-list fast path -- against a real dg.Definitions
    object, the same resolution build_defs() uses."""

    @dg.asset(group_name="electricera_snowpipes")
    def site_details_pipe():
        ...

    @dg.asset(group_name="other")
    def unrelated():
        ...

    sibling_defs = dg.Definitions(assets=[site_details_pipe, unrelated])
    discovered_keys = [
        key.to_user_string() for a in sibling_defs.assets for key in a.keys
    ]

    matched = mod._resolve_selection("group:electricera_snowpipes", discovered_keys, sibling_defs)
    assert matched == ["site_details_pipe"]


def test_empty_match_raises_clear_error():
    comp = _component(monitored_selection="tag:nope=nope")
    with pytest.raises(ValueError, match="matched no assets"):
        comp.build_defs(context=None)


def test_invalid_default_status_raises():
    comp = _component(default_status="bogus")
    with pytest.raises(ValueError, match="default_status"):
        comp.build_defs(context=None)


def test_sensor_fires_run_request_when_monitored_asset_materializes():
    @dg.asset(key=dg.AssetKey(["upstream", "a"]))
    def upstream_a():
        ...

    @dg.job(name="downstream_job")
    def downstream_job():
        ...

    comp = _component(monitored_selection=["upstream/a"], job_name="downstream_job", default_status="running")
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors

    full_defs = dg.Definitions(assets=[upstream_a], jobs=[downstream_job], sensors=[sensor])
    instance = dg.DagsterInstance.ephemeral()
    monitored = dg.AssetSelection.assets(dg.AssetKey(["upstream", "a"]))

    # No materialization yet -- sensor should find nothing to fire on.
    context = dg.build_multi_asset_sensor_context(
        monitored_assets=monitored, instance=instance, definitions=full_defs,
    )
    run_requests = list(sensor(context))
    assert run_requests == []

    # Materialize the monitored asset for real, then re-evaluate.
    dg.materialize([upstream_a], instance=instance)
    context = dg.build_multi_asset_sensor_context(
        monitored_assets=monitored, instance=instance, definitions=full_defs,
    )
    run_requests = list(sensor(context))
    assert len(run_requests) == 1
    assert run_requests[0].job_name == "downstream_job"
