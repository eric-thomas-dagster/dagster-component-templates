"""backfill_policy tests for SubscriptionMetricsComponent.

The Stripe upstream (bound to the `stripe` input) is filtered to the current
partition (partition_date_column) before the MRR / churn / LTV metrics are
computed. A single_run backfill hands the asset a partition RANGE, so the
metrics run once per partition and the one-row frames are concatenated with a
'_partition_key' column.
Real dg.materialize() with a real subscriptions DataFrame; nothing is mocked.

Also pins two pre-existing crashes fixed alongside: ltv_method='predictive'
(NameError on monthly_churn_rate) and a Stripe frame with no plan-amount
column (UnboundLocalError on amount_col).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "stripe_subs"
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


def _epoch(days_ago):
    return int((pd.Timestamp.now() - pd.Timedelta(days=days_ago)).timestamp())


def _frame(with_amount=True):
    rows = []
    for d, day in enumerate(DAYS):
        for i in range(d + 3):
            canceled = i == 0  # one churned sub per day, canceled recently
            row = {
                "loaded_on": day,
                "_resource_type": "subscriptions",
                "status": "canceled" if canceled else "active",
                "created": _epoch(200 + i),
                "current_period_start": _epoch(10),
                "current_period_end": _epoch(-20),
                "canceled_at": _epoch(15) if canceled else None,
                "plan_interval": "year" if i == 1 else "month",
            }
            if with_amount:
                row["plan_amount"] = 1000 * (d + 1) + 500 * i
            rows.append(row)
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="sub_metrics",
        stripe_data_asset_key=UPSTREAM,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="loaded_on",
    )
    kwargs.update(overrides)
    return mod.SubscriptionMetricsComponent(**kwargs)


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


def test_single_partition_still_computes_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("sub_metrics")
    assert "_partition_key" not in out.columns
    assert len(out) == 1
    # 2024-01-03 has 4 subscriptions, one of them canceled (period_end is in the future so still active)
    assert out["active_subscriptions"].iloc[0] == 4
    assert out["churned_subscriptions"].iloc[0] == 1


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("sub_metrics")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node("sub_metrics")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
    counts = out.set_index("_partition_key")["active_subscriptions"]
    assert list(counts) == [3, 4, 5]  # per-day inputs genuinely differ


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("sub_metrics")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("sub_metrics")
    assert "_partition_key" not in out.columns
    assert len(out) == 1 and out["active_subscriptions"].iloc[0] == 3 + 4 + 5
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frame()
    ).output_for_node("sub_metrics")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))


def test_predictive_ltv_does_not_crash(mod):
    component = _component(mod, ltv_method="predictive", partition_type=None, partition_start=None, partition_date_column=None)
    result = _run(component, _frame())
    assert result.success
    assert result.output_for_node("sub_metrics")["ltv"].iloc[0] > 0


def test_stripe_frame_without_amount_column_does_not_crash(mod):
    component = _component(mod, partition_type=None, partition_start=None, partition_date_column=None)
    result = _run(component, _frame(with_amount=False))
    assert result.success
    assert result.output_for_node("sub_metrics")["mrr"].iloc[0] == 0
