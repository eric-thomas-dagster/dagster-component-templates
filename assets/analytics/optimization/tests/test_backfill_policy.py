"""backfill_policy tests for OptimizationComponent.

The component filters its upstream DataFrame to the current partition
(partition_date_column) before solving the LP. A single_run backfill hands it a
partition RANGE, on which context.partition_key raises, so the LP is solved once
per partition in the range and the frames are concatenated.

Everything runs through real dg.materialize() with real DataFrames. The
upstream is served by a tiny IOManager (dg.InMemoryIOManager semantics) because
the upstream asset is external to the materialize call; nothing is mocked.
"""
import importlib.util

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

REQUIRES = "scipy"  # optional dependency the analysis itself needs ("" = none)
needs_deps = pytest.mark.skipif(
    bool(REQUIRES) and importlib.util.find_spec(REQUIRES) is None,
    reason=f"{REQUIRES} is not installed",
)
UPSTREAM = "portfolio_problem"
ASSET = "portfolio_opt"
DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")


@pytest.fixture()
def mod(monkeypatch):
    module = load_component_module()
    return module


class _FrameIO(dg.InMemoryIOManager):
    """InMemoryIOManager that serves `frame` for the external upstream asset."""

    def __init__(self, frame):
        super().__init__()
        self._frame = frame

    def load_input(self, context):
        if context.asset_key == dg.AssetKey(UPSTREAM):
            return self._frame
        return super().load_input(context)


def _frame():
    rows = []
    # one LP per day: 3 variables whose upper bound (and so the optimum) depends on the day
    for d, day in enumerate(DAYS):
        for i, ret in enumerate((0.05, 0.08, 0.11)):
            rows.append({
                "asset": f"A{i}",
                "activity_date": day,
                "expected_return": ret,
                "min_weight": 0.0,
                "max_weight": 0.2 + 0.1 * d + 0.05 * i,
            })
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="portfolio_opt",
        upstream_asset_key=UPSTREAM,
        objective_column="expected_return",
        bounds_lower_column="min_weight",
        bounds_upper_column="max_weight",
        maximize=True,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="activity_date",
    )
    kwargs.update(overrides)
    return mod.OptimizationComponent(**kwargs)


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


def _unpartitioned(mod):
    return _component(mod, partition_type=None, partition_start=None, partition_date_column=None)


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


@needs_deps
def test_single_partition_still_analyses_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node(ASSET)
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0
    assert len(out) == 3  # only that day's 3 variables were solved
    assert list(out["optimal_value"]) == pytest.approx([0.3, 0.35, 0.4])  # = each variable's max_weight that day


@needs_deps
def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node(ASSET)
    assert set(out["_partition_key"]) == set(DAYS)

    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node(ASSET)
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
    # the per-day blocks genuinely differ (the filter ran, not N copies of one result)
    blocks = [out[out["_partition_key"] == k].drop(columns="_partition_key").reset_index(drop=True) for k in DAYS]
    assert not (blocks[0].equals(blocks[1]) and blocks[1].equals(blocks[2]))


@needs_deps
def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node(ASSET)
    assert set(out["_partition_key"]) == {"2024-01-03", "2024-01-04"}


@needs_deps
def test_range_without_filter_columns_runs_once(mod):
    """No partition_date_column -> the key never changes the computation, so a
    range must run the analysis once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node(ASSET)
    assert "_partition_key" not in out.columns
    unpartitioned = _run(_unpartitioned(mod), _frame()).output_for_node(ASSET)
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
