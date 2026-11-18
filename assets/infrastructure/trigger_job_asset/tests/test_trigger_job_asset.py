"""TriggerJobAssetComponent: a REAL, synchronous dagster.execute_job run against a real on-disk
DagsterInstance (dagster.execute_job itself is not mocked -- only run_config forces in-process
execution, a test-only accommodation; see conftest.py).
"""
import dagster as dg
import pytest

from .conftest import IN_PROCESS_RUN_CONFIG


def _materialize_trigger(mod, instance, **attrs):
    component = mod.TriggerJobAssetComponent(asset_key="d/trigger", **attrs)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, instance=instance)


def test_run_job_by_name_real_run(mod, instance, defs_module_spec):
    result = _materialize_trigger(
        mod, instance, definitions_module=defs_module_spec, job_name="named_job",
        run_config=IN_PROCESS_RUN_CONFIG,
    )
    assert result.success
    md = result.asset_materializations_for_node("d__trigger")[0].metadata
    assert md["success"].value is True
    assert md["triggered_job_name"].value == "named_job"
    triggered_run_id = md["triggered_run_id"].value
    triggered_run = instance.get_run_by_id(triggered_run_id)
    assert triggered_run is not None
    assert triggered_run.job_name == "named_job"
    assert triggered_run.tags.get("dagster-community/triggered-by")


def test_run_ad_hoc_asset_selection_real_run(mod, instance, defs_module_spec):
    result = _materialize_trigger(
        mod, instance, definitions_module=defs_module_spec, asset_keys=["a", "b"],
        run_config=IN_PROCESS_RUN_CONFIG,
    )
    assert result.success
    md = result.asset_materializations_for_node("d__trigger")[0].metadata
    assert md["success"].value is True
    triggered_run_id = md["triggered_run_id"].value
    triggered_run = instance.get_run_by_id(triggered_run_id)
    assert triggered_run is not None


def test_triggered_job_failure_fails_the_trigger_asset(mod, instance, defs_module_spec):
    component = mod.TriggerJobAssetComponent(
        asset_key="d/trigger", definitions_module=defs_module_spec, asset_keys=["boom"],
        run_config=IN_PROCESS_RUN_CONFIG,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, instance=instance, raise_on_error=False,
    )
    assert not result.success


def test_extra_tags_land_on_the_triggered_run(mod, instance, defs_module_spec):
    result = _materialize_trigger(
        mod, instance, definitions_module=defs_module_spec, job_name="named_job",
        run_config=IN_PROCESS_RUN_CONFIG, tags={"team": "support"},
    )
    md = result.asset_materializations_for_node("d__trigger")[0].metadata
    triggered_run = instance.get_run_by_id(md["triggered_run_id"].value)
    assert triggered_run.tags.get("team") == "support"


def test_partition_key_lands_as_a_run_tag(mod, instance, defs_module_spec):
    result = _materialize_trigger(
        mod, instance, definitions_module=defs_module_spec, job_name="named_job",
        run_config=IN_PROCESS_RUN_CONFIG, partition_key="2024-01-01",
    )
    md = result.asset_materializations_for_node("d__trigger")[0].metadata
    triggered_run = instance.get_run_by_id(md["triggered_run_id"].value)
    assert triggered_run.tags.get("dagster/partition") == "2024-01-01"


# --- wait=False: fire-and-forget --------------------------------------------------------

def test_wait_false_returns_immediately_with_a_real_run_id(mod, instance, defs_module_spec):
    import time
    component = mod.TriggerJobAssetComponent(
        asset_key="d/trigger", definitions_module=defs_module_spec, job_name="slow_job", wait=False,
        run_config=IN_PROCESS_RUN_CONFIG,
    )
    asset_def = component.build_defs(context=None).assets[0]
    start = time.monotonic()
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, instance=instance)
    elapsed = time.monotonic() - start
    assert result.success
    assert elapsed < 1  # slow_job sleeps 1s -- this must not have waited for it

    md = result.asset_materializations_for_node("d__trigger")[0].metadata
    assert md["waited"].value is False
    run_id = md["triggered_run_id"].value
    assert run_id

    # Give the background thread a moment to actually finish the triggered run, then
    # confirm it really executed (not just created-and-abandoned).
    for _ in range(50):
        triggered_run = instance.get_run_by_id(run_id)
        if triggered_run.is_finished:
            break
        time.sleep(0.1)
    assert triggered_run.is_finished
    assert triggered_run.status == dg.DagsterRunStatus.SUCCESS


def test_wait_false_rejects_timeout_seconds(mod):
    with pytest.raises(Exception, match="timeout_seconds"):
        mod.TriggerJobAssetComponent(
            asset_key="d/trigger", definitions_module="fixture_defs:defs", job_name="named_job",
            wait=False, timeout_seconds=5,
        )


def test_no_pool_by_default(mod, instance, defs_module_spec):
    component = mod.TriggerJobAssetComponent(asset_key="d/trigger", definitions_module=defs_module_spec, job_name="named_job")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool is None


def test_pool_wires_through(mod, instance, defs_module_spec):
    component = mod.TriggerJobAssetComponent(
        asset_key="d/trigger", definitions_module=defs_module_spec, job_name="named_job", pool="trigger_slots",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "trigger_slots"


def test_retry_policy_wires_through(mod, instance, defs_module_spec):
    component = mod.TriggerJobAssetComponent(
        asset_key="d/trigger", definitions_module=defs_module_spec, job_name="named_job",
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_triggered_job(mod, instance, defs_module_spec):
    import time
    component = mod.TriggerJobAssetComponent(
        asset_key="d/trigger", definitions_module=defs_module_spec, job_name="slow_job",
        run_config=IN_PROCESS_RUN_CONFIG, timeout_seconds=0.05,
    )
    asset_def = component.build_defs(context=None).assets[0]
    start = time.monotonic()
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, instance=instance, raise_on_error=False,
    )
    assert not result.success
    assert time.monotonic() - start < 1


def test_requires_exactly_one_of_job_name_or_asset_keys(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.TriggerJobAssetComponent(asset_key="d/trigger", definitions_module="fixture_defs:defs")


def test_rejects_both_job_name_and_asset_keys(mod):
    with pytest.raises(Exception, match="exactly one"):
        mod.TriggerJobAssetComponent(
            asset_key="d/trigger", definitions_module="fixture_defs:defs",
            job_name="named_job", asset_keys=["a"],
        )
