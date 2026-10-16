"""backfill_policy tests for RevenueAttributionComponent.

The component has no declared ins: it loads its marketing / revenue upstreams
with context.load_asset_value and filters each to the current partition
(partition_date_column). A single_run backfill hands it a partition RANGE, so
the analysis runs once per partition and the frames are concatenated.
Real dg.materialize() with real DataFrames; both upstreams are served by a tiny
IOManager (the upstream assets are external to the materialize call).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

MARKETING = "marketing_data"
REVENUE = "revenue_data"
DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")


@pytest.fixture()
def mod():
    return load_component_module()


class _FramesIO(dg.InMemoryIOManager):
    def __init__(self, frames):
        super().__init__()
        self._frames = frames

    def load_input(self, context):
        name = context.asset_key.to_user_string()
        if name in self._frames:
            return self._frames[name]
        return super().load_input(context)


def _frames():
    mrows, rrows = [], []
    for d, day in enumerate(DAYS):
        for c, camp in enumerate(("search", "social")):
            mrows.append({
                "date": day,
                "campaign_name": camp,
                "spend": 100.0 * (d + 1) + 10 * c,
                "impressions": 1000 * (d + 1),
                "clicks": 50 + 5 * d + c,
                "conversions": d + c + 1,
            })
        rrows.append({"date": day, "email": f"u{d}@x.com", "amount": 25.0 * (d + 1)})
    return {MARKETING: pd.DataFrame(mrows), REVENUE: pd.DataFrame(rrows)}


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="attribution",
        marketing_data_asset_key=MARKETING,
        revenue_data_asset_key=REVENUE,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="date",
    )
    kwargs.update(overrides)
    return mod.RevenueAttributionComponent(**kwargs)


def _asset(component):
    return component.build_defs(context=None).assets[0]


def _run(component, frames, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component), dg.SourceAsset(MARKETING), dg.SourceAsset(REVENUE)],
        resources={"io_manager": _FramesIO(frames)},
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
    result = _run(_component(mod, backfill_policy="single_run"), _frames(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("attribution")
    assert "_partition_key" not in out.columns
    assert len(out) == 2
    assert set(out["spend"]) == {200.0, 210.0}  # only 2024-01-03's spend


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frames = _frames()
    ranged = _run(component, frames, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("attribution")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frames, partition_key=key).output_for_node("attribution")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frames(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("attribution")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frames(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("attribution")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frames()
    ).output_for_node("attribution")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
