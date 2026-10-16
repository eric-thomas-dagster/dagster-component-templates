"""backfill_policy tests for SurvivalAnalysisComponent.

The upstream frame is filtered to the current partition (partition_date_column)
before fitting. A single_run backfill hands the asset a partition RANGE, so the
model is fitted once per partition and the frames are concatenated with a
'_partition_key' column.
Real lifelines + real dg.materialize(); nothing is mocked. Skipped when
lifelines is not installed.
"""
import dagster as dg
import pandas as pd
import pytest

pytest.importorskip("lifelines")

from .conftest import load_component_module  # noqa: E402

UPSTREAM = "subjects"
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
        for i in range(12):
            rows.append({
                "cohort_day": day,
                "tenure": float(1 + i + d * 2 + (i % 3)),
                "churned": 1 if (i + d) % 3 else 0,
                "age": float(20 + i * 2 + d),
                "plan": "pro" if i % 2 else "basic",
            })
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="survival",
        upstream_asset_key=UPSTREAM,
        duration_column="tenure",
        event_column="churned",
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="cohort_day",
    )
    kwargs.update(overrides)
    return mod.SurvivalAnalysisComponent(**kwargs)


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


def test_single_partition_still_fits_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("survival")
    assert "_partition_key" not in out.columns
    assert list(out.columns) == ["timeline", "survival_probability"]
    # 2024-01-03 tenures start at 1 + 2 = 3, so the curve has no timeline point below 3
    assert out["timeline"].max() <= 1 + 11 + 2 + 2 and out["timeline"].iloc[1] >= 3


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("survival")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node("survival")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
    first_event_time = out.groupby("_partition_key")["timeline"].apply(lambda s: sorted(s)[1])
    assert first_event_time.is_monotonic_increasing and first_event_time.nunique() > 1  # per-day inputs differ


def test_single_run_range_with_groups_and_cox(mod):
    grouped = _component(mod, backfill_policy="single_run", group_column="plan")
    out = _run(grouped, _frame(), tags=_range_tags(DAYS[0], DAYS[1])).output_for_node("survival")
    assert set(out["_partition_key"]) == {DAYS[0], DAYS[1]}
    assert set(out["plan"]) == {"basic", "pro"}

    cox = _component(mod, backfill_policy="single_run", method="cox", covariate_columns=["age"])
    out = _run(cox, _frame(), tags=_range_tags(DAYS[0], DAYS[1])).output_for_node("survival")
    assert set(out["_partition_key"]) == {DAYS[0], DAYS[1]}
    assert (out["covariate"] == "age").all()


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("survival")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("survival")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frame()
    ).output_for_node("survival")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
