"""Real, committed tests for DynamicFanoutAssetComponent -- real dg.materialize() throughout,
no mocking (discover/process/collect are plain Python, free/local). Covers both the
pre-existing discover_callable_path path and the items_from literal-list/asset-key shortcut.
"""
import importlib

import dagster as dg
import pytest

from .conftest import materialize

FIXTURES_PATH = "assets.transforms.dynamic_fanout_asset.tests.fixture_callables"


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


# --- pre-existing discover_callable_path path (regression) -----------------------------

def test_discover_callable_path_process_and_collect(mod, fx):
    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout",
        discover_callable_path=f"{FIXTURES_PATH}:discover_static",
        process_callable_path=f"{FIXTURES_PATH}:double",
        collect_callable_path=f"{FIXTURES_PATH}:total",
    )
    result = materialize(component)
    assert result.success
    assert sorted(fx.CALLS["processed"]) == [1, 2, 3]
    assert result.output_for_node("fanout", output_name="result") == 12  # (1+2+3)*2


def test_discover_callable_path_with_upstream_asset(mod, fx, tmp_path):
    # load_asset_value() (used by discover's `upstream=` passthrough) reads a persisted
    # materialization -- an ephemeral InMemoryIOManager doesn't support that read-back
    # within the SAME run, so this needs a real on-disk IO manager, unlike every other
    # test here which only needs upstream VALUES passed directly into an asset's inputs.
    @dg.asset(key=dg.AssetKey.from_user_string("source"))
    def source():
        return [1, 2]

    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout",
        upstream_asset_key="source",
        discover_callable_path=f"{FIXTURES_PATH}:discover_from_upstream",
        process_callable_path=f"{FIXTURES_PATH}:double",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [source, asset_def], resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
    )
    assert result.success
    assert fx.CALLS["upstream"] == [1, 2]
    # discover_from_upstream: [10, 20]; doubled -> [20, 40]
    assert sorted(result.output_for_node("fanout", output_name="result")) == [20, 40]


def test_fail_on_empty_raises(mod, fx):
    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout",
        discover_callable_path=f"{FIXTURES_PATH}:discover_static",
        process_callable_path=f"{FIXTURES_PATH}:double",
        fail_on_empty=True,
    )
    # discover_static always returns [1,2,3] -- swap in a zero-item callable via kwargs trick
    # is unnecessary; instead verify fail_on_empty is wired by using items_from=[] directly.
    component2 = mod.DynamicFanoutAssetComponent(
        asset_name="fanout", items_from=[], process_callable_path=f"{FIXTURES_PATH}:double",
        fail_on_empty=True,
    )
    result = materialize(component2, raise_on_error=False)
    assert not result.success


# --- items_from: literal list ----------------------------------------------------------

def test_items_from_literal_list_skips_discover_callable(mod, fx):
    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout", items_from=[5, 6, 7], process_callable_path=f"{FIXTURES_PATH}:double",
        collect_callable_path=f"{FIXTURES_PATH}:total",
    )
    result = materialize(component)
    assert result.success
    assert sorted(fx.CALLS["processed"]) == [5, 6, 7]
    assert result.output_for_node("fanout", output_name="result") == 36  # (5+6+7)*2


def test_items_from_empty_literal_list_no_items_no_failure_by_default(mod):
    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout", items_from=[], process_callable_path=f"{FIXTURES_PATH}:double",
    )
    result = materialize(component)
    assert result.success
    assert result.output_for_node("fanout", output_name="result") == []


# --- items_from: upstream asset key (string) --------------------------------------------

def test_items_from_asset_key_uses_upstream_value_as_the_items_list(mod, fx, tmp_path):
    @dg.asset(key=dg.AssetKey.from_user_string("source"))
    def source():
        return [1, 2, 3]

    component = mod.DynamicFanoutAssetComponent(
        asset_name="fanout", items_from="source", process_callable_path=f"{FIXTURES_PATH}:double",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [source, asset_def], resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
    )
    assert result.success
    assert sorted(fx.CALLS["processed"]) == [1, 2, 3]
    assert sorted(result.output_for_node("fanout", output_name="result")) == [2, 4, 6]


# --- validation ---------------------------------------------------------------------

def test_requires_discover_callable_path_or_items_from(mod):
    with pytest.raises(Exception, match="discover_callable_path or items_from"):
        mod.DynamicFanoutAssetComponent(asset_name="fanout", process_callable_path="x:y")


def test_rejects_both_discover_callable_path_and_items_from(mod):
    with pytest.raises(Exception, match="not both"):
        mod.DynamicFanoutAssetComponent(
            asset_name="fanout", discover_callable_path="x:y", items_from=[1],
            process_callable_path="x:y",
        )


def test_rejects_items_from_asset_key_together_with_upstream_asset_key(mod):
    with pytest.raises(Exception, match="redundant"):
        mod.DynamicFanoutAssetComponent(
            asset_name="fanout", items_from="source", upstream_asset_key="other",
            process_callable_path="x:y",
        )
