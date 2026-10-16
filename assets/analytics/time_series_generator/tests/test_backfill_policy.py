"""backfill_policy tests for TimeSeriesGeneratorComponent.

This component has no upstream: a daily partition key selects the day to generate. The partition_date_column
field is what switches single_run into per-partition looping. A single_run backfill hands it a partition
RANGE, so one day is generated per partition key and the frames are concatenated (random_state is fixed,
so each block equals that day's own single run).

Everything runs through real dg.materialize() with real data.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()



def _component(mod, **overrides):
    kwargs = dict(
        asset_name="ts_gen",
        pattern_type="complex",
        frequency="1h",
        base_value=100.0,
        noise_level=0.1,
        random_state=42,
        metric_name="temperature",
        start_date="2024-01-01",
        end_date="2024-01-03",
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="d",
    )
    kwargs.update(overrides)
    return mod.TimeSeriesGeneratorComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frame=None, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component)],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def _range_tags(start, end):
    return {
        "dagster/asset_partition_range_start": start,
        "dagster/asset_partition_range_end": end,
    }


def _unpartitioned(mod, **overrides):
    return _component(mod, partition_type=None, partition_start=None, partition_values=None, **overrides)


def test_backfill_policy_is_wired_onto_the_asset(mod):
    assert _asset(_component(mod, backfill_policy="single_run")).backfill_policy == dg.BackfillPolicy.single_run()
    assert _asset(
        _component(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3)
    ).backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    assert _asset(_component(mod)).backfill_policy is None


def test_backfill_policy_ignored_when_not_partitioned(mod):
    asset_def = _asset(_unpartitioned(mod, backfill_policy="single_run"))
    assert asset_def.partitions_def is None
    assert asset_def.backfill_policy is None


def test_single_partition_run_is_ordinary(mod):
    result = _run(_component(mod, backfill_policy="single_run"), None, partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("ts_gen")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frame = None
    ranged = _run(component, frame, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node("ts_gen")
    assert set(out["_partition_key"]) == {'2024-01-03', '2024-01-04', '2024-01-02'}

    for key in ('2024-01-02', '2024-01-03', '2024-01-04'):
        single = _run(component, frame, partition_key=key).output_for_node("ts_gen")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        assert len(block) > 0
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, None, tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node("ts_gen")
    assert set(out["_partition_key"]) == {'2024-01-03', '2024-01-04'}


def test_range_without_filter_columns_runs_once(mod):
    """No partition filter column -> the key never changes the computation, so a
    range must run the analysis once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None, partition_static_column=None)
    out = _run(component, None, tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("ts_gen")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(_unpartitioned(mod, partition_date_column=None, partition_static_column=None), None).output_for_node("ts_gen")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))


def test_each_block_is_that_days_24_hours(mod):
    out = _run(_component(mod, backfill_policy="single_run"),
               tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("ts_gen")
    assert len(out) == 72
    for key, block in out.groupby("_partition_key"):
        assert len(block) == 24
        assert block["timestamp"].dt.strftime("%Y-%m-%d").eq(key).all()
