"""Real, committed tests for get_upstream_value -- reading a value/metadata entry from ANY
asset by key, not necessarily a declared dependency. Real dg.materialize() throughout (a real
FilesystemIOManager for the full-value-load cases, since load_asset_value() needs a persisted
IO manager, not InMemoryIOManager -- confirmed earlier this session for dynamic_fanout_asset's
own upstream_asset_key path).
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _upstream_asset(key: str, value=None, metadata=None):
    @dg.asset(key=dg.AssetKey.from_user_string(key))
    def _a():
        if metadata is not None:
            return dg.MaterializeResult(metadata=metadata)
        return value
    return _a


# --- metadata_key set: reads one named metadata entry ------------------------------------

def test_reads_a_named_metadata_entry_from_this_run(mod):
    upstream = _upstream_asset("up/a", metadata={"row_count": dg.MetadataValue.int(42)})

    @dg.asset(deps=[dg.AssetKey.from_user_string("up/a")])
    def consumer(context):
        return mod.get_upstream_value(context, "up/a", metadata_key="row_count")

    result = dg.materialize([upstream, consumer], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert result.output_for_node("consumer") == 42


def test_missing_metadata_key_returns_default(mod):
    upstream = _upstream_asset("up/a", metadata={"other": dg.MetadataValue.int(1)})

    @dg.asset(deps=[dg.AssetKey.from_user_string("up/a")])
    def consumer(context):
        return mod.get_upstream_value(context, "up/a", metadata_key="row_count", default=0)

    result = dg.materialize([upstream, consumer], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert result.output_for_node("consumer") == 0


def test_missing_metadata_key_no_default_fails(mod):
    upstream = _upstream_asset("up/a", metadata={"other": dg.MetadataValue.int(1)})

    @dg.asset(deps=[dg.AssetKey.from_user_string("up/a")])
    def consumer(context):
        return mod.get_upstream_value(context, "up/a", metadata_key="row_count")

    result = dg.materialize(
        [upstream, consumer], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success


def test_scope_this_run_rejects_a_prior_runs_metadata(mod):
    upstream = _upstream_asset("up/a", metadata={"row_count": dg.MetadataValue.int(99)})
    instance = dg.DagsterInstance.ephemeral()
    # Materialize upstream ALONE first, in its own run -- not part of the consumer's run.
    dg.materialize([upstream], instance=instance, resources={"io_manager": dg.InMemoryIOManager()})

    @dg.asset
    def consumer(context):
        return mod.get_upstream_value(context, "up/a", metadata_key="row_count", default="fallback")

    result = dg.materialize([consumer], instance=instance, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert result.output_for_node("consumer") == "fallback"  # this_run scope -- prior run doesn't count


def test_scope_latest_accepts_a_prior_runs_metadata(mod):
    upstream = _upstream_asset("up/a", metadata={"row_count": dg.MetadataValue.int(99)})
    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([upstream], instance=instance, resources={"io_manager": dg.InMemoryIOManager()})

    @dg.asset
    def consumer(context):
        return mod.get_upstream_value(context, "up/a", metadata_key="row_count", scope="latest")

    result = dg.materialize([consumer], instance=instance, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert result.output_for_node("consumer") == 99


# --- metadata_key unset: loads the full IO-manager value ----------------------------------

def test_loads_the_full_value_via_io_manager_this_run(mod, tmp_path):
    upstream = _upstream_asset("up/a", value=[1, 2, 3])

    @dg.asset(deps=[dg.AssetKey.from_user_string("up/a")])
    def consumer(context):
        return mod.get_upstream_value(context, "up/a")

    result = dg.materialize(
        [upstream, consumer], resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
    )
    assert result.success
    assert result.output_for_node("consumer") == [1, 2, 3]


def test_value_load_no_materialization_ever_no_default_fails(mod, tmp_path):
    @dg.asset
    def consumer(context):
        return mod.get_upstream_value(context, "up/never_materialized")

    result = dg.materialize(
        [consumer], resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))}, raise_on_error=False,
    )
    assert not result.success


def test_value_load_no_materialization_ever_with_default_succeeds(mod, tmp_path):
    @dg.asset
    def consumer(context):
        return mod.get_upstream_value(context, "up/never_materialized", default="nope")

    result = dg.materialize(
        [consumer], resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
    )
    assert result.success
    assert result.output_for_node("consumer") == "nope"
