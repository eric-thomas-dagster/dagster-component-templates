"""backfill_policy tests for SimulationSamplingComponent.

The upstream frame defines one distribution per row; it is filtered to the
current partition (partition_date_column) before simulating. A single_run
backfill hands the asset a partition RANGE, so the simulation runs once per
partition and the frames are concatenated with a '_partition_key' column.
Real dg.materialize() with real DataFrames (seeded numpy, nothing mocked).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "distributions"
DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")


@pytest.fixture()
def mod():
    return load_component_module()


class _FrameIO(dg.InMemoryIOManager):
    def __init__(self, frame):
        super().__init__()
        self._frame = frame

    def load_input(self, context):
        if context.asset_key == dg.AssetKey(UPSTREAM):
            return self._frame
        return super().load_input(context)


def _frame():
    rows = []
    for d, day in enumerate(DAYS):
        rows.append({"as_of": day, "var": "demand", "dist": "normal", "param1": 100.0 * (d + 1), "param2": 5.0 + d})
        rows.append({"as_of": day, "var": "price", "dist": "uniform", "param1": 1.0 + d, "param2": 4.0 + d})
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="sims",
        upstream_asset_key=UPSTREAM,
        variable_column="var",
        distribution_column="dist",
        param1_column="param1",
        param2_column="param2",
        n_simulations=20,
        output_mode="statistics",
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="as_of",
    )
    kwargs.update(overrides)
    return mod.SimulationSamplingComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frame, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component), dg.SourceAsset(UPSTREAM)],
        resources={"io_manager": _FrameIO(frame)},
        partition_key=partition_key,
        tags=tags or {},
    )


def _range_tags(start, end):
    return {
        "dagster/asset_partition_range_start": start,
        "dagster/asset_partition_range_end": end,
    }


def test_backfill_policy_is_wired_onto_the_asset(mod):
    assert _asset(_component(mod, backfill_policy="single_run")).backfill_policy == dg.BackfillPolicy.single_run()
    assert _asset(
        _component(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3)
    ).backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    assert _asset(_component(mod)).backfill_policy is None


def test_backfill_policy_ignored_when_not_partitioned(mod):
    asset_def = _asset(_component(mod, partition_type=None, partition_start=None, backfill_policy="single_run"))
    assert asset_def.partitions_def is None
    assert asset_def.backfill_policy is None


def test_single_partition_still_simulates_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("sims")
    assert "_partition_key" not in out.columns
    assert set(out["variable"]) == {"demand", "price"}
    # demand ~ Normal(200, 6): only the 2024-01-03 distribution was used
    assert 150 < out.loc[out["variable"] == "demand", "mean"].iloc[0] < 250


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("sims")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node("sims")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
    # the per-day distributions genuinely differ
    demand = out[out["variable"] == "demand"].set_index("_partition_key")["mean"]
    assert demand["2024-01-02"] < demand["2024-01-03"] < demand["2024-01-04"]


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("sims")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("sims")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frame()
    ).output_for_node("sims")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
