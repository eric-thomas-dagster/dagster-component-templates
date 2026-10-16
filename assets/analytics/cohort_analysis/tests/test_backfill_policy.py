"""backfill_policy tests for CohortAnalysisComponent.

The component filters its upstream DataFrame to the current partition
(partition_date_column) before analysing it. A single_run backfill hands it a
partition RANGE, on which context.partition_key raises, so the analysis runs
once per partition in the range and the frames are concatenated.

Everything runs through real dg.materialize() with real DataFrames. The
upstream is served by a tiny IOManager (dg.InMemoryIOManager semantics) because
the upstream asset is external to the materialize call; nothing is mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "customer_activity"


@pytest.fixture()
def mod():
    return load_component_module()


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
    # (customer, first_order, activity_date, revenue); activity_date drives partitions.
    for day, n in (("2024-01-02", 2), ("2024-01-03", 3), ("2024-01-04", 4)):
        for i in range(n):
            rows.append({
                "customer_id": f"{day}-{i}",
                "first_order_date": "2024-01-01",
                "activity_date": day,
                "revenue": 10.0 + i,
            })
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="cohorts",
        upstream_asset_key=UPSTREAM,
        cohort_period="daily",
        retention_periods=3,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="activity_date",
    )
    kwargs.update(overrides)
    return mod.CohortAnalysisComponent(**kwargs)


def _asset(component):
    return component.build_defs(context=None).assets[0]


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


def test_single_partition_still_analyses_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("cohorts")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node("cohorts")
    assert set(out["_partition_key"]) == {"2024-01-02", "2024-01-03", "2024-01-04"}

    for key in ("2024-01-02", "2024-01-03", "2024-01-04"):
        single = _run(component, frame, partition_key=key).output_for_node("cohorts")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node("cohorts")
    assert set(out["_partition_key"]) == {"2024-01-03", "2024-01-04"}


def test_range_without_filter_columns_runs_once(mod):
    """No partition_date_column -> the key never changes the computation, so a
    range must run the analysis once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("cohorts")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(_component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frame()).output_for_node("cohorts")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
