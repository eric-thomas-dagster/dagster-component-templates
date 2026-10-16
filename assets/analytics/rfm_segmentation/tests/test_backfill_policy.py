"""backfill_policy tests for RFMSegmentationComponent.

The component loads its upstream with context.load_asset_value and filters it
to the current partition (partition_date_column = the snapshot/load date).
A single_run backfill hands it a partition RANGE, so the segmentation runs once
per partition and the frames are concatenated.
Real dg.materialize() with real DataFrames; the external upstream is served by
a tiny IOManager.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

UPSTREAM = "transactions"
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
    n = 0
    for d, day in enumerate(DAYS):
        for c in range(12):
            for o in range(1 + (c + d) % 4):
                n += 1
                rows.append({
                    "snapshot_date": day,
                    "customer_id": f"{day}-c{c}",
                    "order_id": f"o{n}",
                    "order_date": (pd.Timestamp("2024-01-01") + pd.Timedelta(days=c * 3 + o + d)).strftime("%Y-%m-%d"),
                    "revenue": 10.0 + c * (d + 1) + o,
                })
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="rfm",
        upstream_asset_key=UPSTREAM,
        lookback_days=20000,  # fixture dates are in 2024; keep them inside the window
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="snapshot_date",
    )
    kwargs.update(overrides)
    return mod.RFMSegmentationComponent(**kwargs)


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


def test_single_partition_still_segments_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("rfm")
    assert "_partition_key" not in out.columns
    assert len(out) == 12
    assert set(out["score_date"]) == {"2024-01-03"}
    assert all(cid.startswith("2024-01-03") for cid in out["customer_id"])


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("rfm")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frame, partition_key=key).output_for_node("rfm")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("rfm")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frame(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("rfm")
    assert "_partition_key" not in out.columns
    # the whole upstream was segmented once (36 distinct customers), not 3 filtered copies
    assert out["customer_id"].nunique() == 36 and len(out) == 36
