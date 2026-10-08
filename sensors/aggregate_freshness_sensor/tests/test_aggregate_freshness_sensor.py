import importlib.util
import pathlib

import dagster as dg
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "aggregate_freshness_sensor_component", here / "component.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
AggregateFreshnessSensorComponent = mod.AggregateFreshnessSensorComponent


def _component(**overrides):
    attrs = dict(
        sensor_name="test_aggregate_sensor",
        monitored_selection=["sap/bseg", "sap/konv"],
        rollup_asset_key="sap/hourly_feed_health",
        max_silence_seconds=600,
    )
    attrs.update(overrides)
    return AggregateFreshnessSensorComponent(**attrs)


def test_explicit_list_selection_builds_a_real_sensor_and_rollup_asset():
    comp = _component()
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors
    (rollup_asset,) = defs.assets
    assert sensor.name == "test_aggregate_sensor"
    assert rollup_asset.key == dg.AssetKey(["sap", "hourly_feed_health"])


def test_selection_dsl_resolves_against_real_sibling_assets():
    """Exercises the real AssetSelection.from_string() path (group:), not
    just the explicit-list fast path -- against a real dg.Definitions
    object, the same resolution build_defs() uses."""

    @dg.asset(group_name="sap_hourly")
    def bseg():
        ...

    @dg.asset(group_name="other")
    def unrelated():
        ...

    sibling_defs = dg.Definitions(assets=[bseg, unrelated])
    discovered_keys = [key.to_user_string() for a in sibling_defs.assets for key in a.keys]

    matched = mod._resolve_selection("group:sap_hourly", discovered_keys, sibling_defs)
    assert matched == ["bseg"]


def test_empty_match_raises_clear_error():
    comp = _component(monitored_selection="tag:nope=nope")
    with pytest.raises(ValueError, match="matched no assets"):
        comp.build_defs(context=None)


def test_invalid_default_status_raises():
    comp = _component(default_status="bogus")
    with pytest.raises(ValueError, match="default_status"):
        comp.build_defs(context=None)


def _run_sensor_once(sensor, instance, definitions):
    context = dg.build_sensor_context(instance=instance, definitions=definitions)
    return sensor(context)


def test_check_fails_when_nothing_ever_materialized():
    @dg.asset(key=dg.AssetKey(["sap", "bseg"]))
    def bseg():
        ...

    @dg.asset(key=dg.AssetKey(["sap", "konv"]))
    def konv():
        ...

    comp = _component(monitored_selection=["sap/bseg", "sap/konv"], default_status="running")
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors
    (rollup_asset,) = defs.assets

    full_defs = dg.Definitions(assets=[bseg, konv, rollup_asset], sensors=[sensor])
    instance = dg.DagsterInstance.ephemeral()

    result = _run_sensor_once(sensor, instance, full_defs)
    (evaluation,) = result.asset_events
    assert isinstance(evaluation, dg.AssetCheckEvaluation)
    assert evaluation.passed is False
    assert evaluation.check_name == "total_stoppage"
    assert evaluation.asset_key == dg.AssetKey(["sap", "hourly_feed_health"])


def test_check_passes_when_one_monitored_asset_recently_materialized():
    @dg.asset(key=dg.AssetKey(["sap", "bseg"]))
    def bseg():
        ...

    @dg.asset(key=dg.AssetKey(["sap", "konv"]))
    def konv():
        ...

    comp = _component(monitored_selection=["sap/bseg", "sap/konv"], default_status="running")
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors
    (rollup_asset,) = defs.assets

    full_defs = dg.Definitions(assets=[bseg, konv, rollup_asset], sensors=[sensor])
    instance = dg.DagsterInstance.ephemeral()

    # Only ONE of the two monitored assets materializes -- the aggregate
    # check should still pass, because *something* in the selection is fresh.
    dg.materialize([bseg], instance=instance)

    result = _run_sensor_once(sensor, instance, full_defs)
    (evaluation,) = result.asset_events
    assert evaluation.passed is True
    assert evaluation.metadata["freshest_asset_key"].text == "sap/bseg"


def test_check_fails_when_freshest_event_exceeds_silence_window():
    @dg.asset(key=dg.AssetKey(["sap", "bseg"]))
    def bseg():
        ...

    comp = _component(
        monitored_selection=["sap/bseg"],
        max_silence_seconds=0,  # anything older than "right now" counts as stale
        default_status="running",
    )
    defs = comp.build_defs(context=None)
    (sensor,) = defs.sensors
    (rollup_asset,) = defs.assets

    full_defs = dg.Definitions(assets=[bseg, rollup_asset], sensors=[sensor])
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([bseg], instance=instance)

    import time

    time.sleep(0.05)
    result = _run_sensor_once(sensor, instance, full_defs)
    (evaluation,) = result.asset_events
    assert evaluation.passed is False
