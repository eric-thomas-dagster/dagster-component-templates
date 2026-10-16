"""backfill_policy tests for ABTestAnalysisComponent.

The component filters its upstream experiment frame to the current partition (partition_date_column)
before running the z-test. A single_run backfill hands it a partition RANGE, so the analysis runs
once per partition in the range and the result frames are concatenated.

Everything runs through real dg.materialize() with real data, the upstream served by a tiny IOManager (the upstream asset is external to the materialize call).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "upstream_data"

def _frame():
    rows = []
    # day -> (control users, control conversions, treatment users, treatment conversions)
    for day, (cn, cc, tn, tc) in {
        "2024-01-02": (10, 2, 10, 5),
        "2024-01-03": (12, 6, 12, 3),
        "2024-01-04": (14, 7, 14, 12),
    }.items():
        for variant, n, conv in (("control", cn, cc), ("treatment", tn, tc)):
            for i in range(n):
                rows.append({
                    "experiment_id": "exp_1",
                    "variant": variant,
                    "user_id": f"{day}-{variant}-{i}",
                    "converted": i < conv,
                    "date": day,
                })
    return pd.DataFrame(rows)


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



def _component(mod, **overrides):
    kwargs = dict(
        asset_name="ab_results",
        upstream_asset_key=UPSTREAM,
        minimum_sample_size=5,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="date",
    )
    kwargs.update(overrides)
    return mod.ABTestAnalysisComponent(**kwargs)


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
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("ab_results")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node("ab_results")
    assert set(out["_partition_key"]) == {'2024-01-03', '2024-01-04', '2024-01-02'}

    for key in ('2024-01-02', '2024-01-03', '2024-01-04'):
        single = _run(component, frame, partition_key=key).output_for_node("ab_results")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        assert len(block) > 0
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True), check_dtype=False)


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node("ab_results")
    assert set(out["_partition_key"]) == {'2024-01-03', '2024-01-04'}


def test_range_without_filter_columns_runs_once(mod):
    """No partition filter column -> the key never changes the computation, so a
    range must run the analysis once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None, partition_static_column=None)
    out = _run(component, _frame(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("ab_results")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(_unpartitioned(mod, partition_date_column=None, partition_static_column=None), _frame()).output_for_node("ab_results")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))


def test_each_partition_analyses_only_its_own_rows(mod):
    """Per-day data differs, so the per-partition counts prove the filter ran for each key."""
    out = _run(_component(mod, backfill_policy="single_run"), _frame(),
               tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("ab_results")
    by_key = out.set_index("_partition_key")
    assert by_key.loc["2024-01-02", "control_n"] == 10
    assert by_key.loc["2024-01-03", "control_n"] == 12
    assert by_key.loc["2024-01-04", "treatment_conversions"] == 12
