"""Real, committed tests for RuntimePartitionAssetComponent -- real dg.materialize() and a
real on-disk-free dg.DagsterInstance.ephemeral() throughout, no mocking (this component has no
external surface beyond the instance's own dynamic-partition/runless-event APIs, which are
exercised for real).
"""
import importlib

import dagster as dg
import pytest

from .conftest import materialize

FIXTURES_PATH = "assets.infrastructure.runtime_partition_asset.tests.fixture_callables"


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def _comp(mod, **kw):
    kw.setdefault("asset_key", "rt/target")
    kw.setdefault("dynamic_partitions_name", "rt_regions")
    return mod.RuntimePartitionAssetComponent(**kw)


# --- validation --------------------------------------------------------------------

def test_run_tag_requires_key_run_tag(mod):
    with pytest.raises(ValueError, match="key_run_tag"):
        _comp(mod, callable_path=f"{FIXTURES_PATH}:plain_compute", key_source="run_tag")


def test_run_config_requires_key_config_field(mod):
    with pytest.raises(ValueError, match="key_config_field"):
        _comp(mod, callable_path=f"{FIXTURES_PATH}:plain_compute", key_source="run_config")


def test_asset_key_and_executor_key_must_differ(mod):
    with pytest.raises(ValueError, match="must be different"):
        _comp(mod, callable_path=f"{FIXTURES_PATH}:choose_region", executor_asset_key="rt/target")


# --- key_source='callable_return' ---------------------------------------------------

def test_callable_return_registers_partition_and_reports_materialization(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:choose_region")
    result = materialize(component, instance)
    assert result.success

    md = result.asset_materializations_for_node("rt__target_runner")[0].metadata
    assert md["chosen_partition_key"].value == "us-east"
    assert md["dynamic_partitions_name"].value == "rt_regions"

    assert instance.has_dynamic_partition("rt_regions", "us-east")
    target_key = dg.AssetKey.from_user_string("rt/target")
    assert instance.get_materialized_partitions(target_key) == {"us-east"}


def test_callable_return_rejects_non_tuple_shape(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:choose_region_bad_shape")
    result = materialize(component, instance, raise_on_error=False)
    assert not result.success


def test_callable_return_does_not_reregister_an_already_known_partition(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:choose_region")
    materialize(component, instance)
    assert instance.get_dynamic_partitions("rt_regions") == ["us-east"]
    # Second run with the same chosen key -- still fine, not a duplicate-registration error.
    result = materialize(component, instance)
    assert result.success
    assert instance.get_dynamic_partitions("rt_regions") == ["us-east"]


def test_callable_return_feeds_taskflow_style_inputs(mod, fx, instance):
    @dg.asset(key=dg.AssetKey.from_user_string("rt/source"))
    def source():
        return 5
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:wants_upstream", inputs={"upstream_a": "rt/source"})
    defs = component.build_defs(context=None)
    runner = [a for a in defs.assets if isinstance(a, dg.AssetsDefinition)][0]
    result = dg.materialize([source, runner], instance=instance, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == 5
    target_key = dg.AssetKey.from_user_string("rt/target")
    assert instance.get_materialized_partitions(target_key) == {"region-5"}


# --- key_source='run_tag' -----------------------------------------------------------

def test_run_tag_source_uses_the_tag_as_the_key(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:plain_compute", key_source="run_tag", key_run_tag="region")
    result = materialize(component, instance, tags={"region": "eu-west"})
    assert result.success
    target_key = dg.AssetKey.from_user_string("rt/target")
    assert instance.get_materialized_partitions(target_key) == {"eu-west"}


def test_run_tag_source_fails_clearly_when_tag_missing(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:plain_compute", key_source="run_tag", key_run_tag="region")
    result = materialize(component, instance, raise_on_error=False)
    assert not result.success


# --- key_source='run_config' --------------------------------------------------------

def test_run_config_source_uses_the_config_field_as_the_key(mod, fx, instance):
    component = _comp(mod, callable_path=f"{FIXTURES_PATH}:plain_compute", key_source="run_config", key_config_field="region")
    defs = component.build_defs(context=None)
    runner = [a for a in defs.assets if isinstance(a, dg.AssetsDefinition)][0]
    result = dg.materialize(
        [runner], instance=instance, resources={"io_manager": dg.InMemoryIOManager()},
        run_config={"ops": {runner.op.name: {"config": {"region": "apac"}}}},
    )
    assert result.success
    target_key = dg.AssetKey.from_user_string("rt/target")
    assert instance.get_materialized_partitions(target_key) == {"apac"}
