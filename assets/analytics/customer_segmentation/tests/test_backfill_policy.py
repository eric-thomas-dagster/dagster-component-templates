"""backfill_policy tests for CustomerSegmentationComponent.

The RFM analysis filters the transaction frame to the current partition
(partition_date_column) before scoring customers.

A single_run backfill hands the asset a partition RANGE, on which
context.partition_key raises, so the computation runs once per partition in the
range and the frames are concatenated with a `_partition_key` column.

Everything runs through real dg.materialize() with real DataFrames. The
upstream asset(s) are external to the materialize call, so a tiny IOManager
(dg.InMemoryIOManager semantics) serves them; nothing is mocked (RFM is pure pandas).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")
VOLATILE = ['segmented_at']  # columns that legitimately differ between two runs (wall-clock etc.)
ASSET = 'segments'


@pytest.fixture()
def mod():
    return load_component_module()


class _FrameIO(dg.InMemoryIOManager):
    """InMemoryIOManager that serves the real upstream DataFrames by asset key."""

    def __init__(self, frames):
        super().__init__()
        self._frames = frames

    def load_input(self, context):
        name = context.asset_key.to_user_string()
        if name in self._frames:
            return self._frames[name]
        return super().load_input(context)


def _frames():
    rows = []
    for d, day in enumerate(DAYS):
        for i in range(10):
            for j in range(i // 2 + 1):  # frequency 1..5 so quintiles are distinct
                rows.append({
                    "customer_id": f"{day}-c{i}",
                    "date": f"2023-{12 - (i % 6):02d}-{10 + j}",
                    "amount": 10.0 * (i + 1) + 3 * d + j,
                    "event_day": day,
                })
    return {"transactions": pd.DataFrame(rows)}


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="segments",
        transaction_data_asset_key="transactions",
        analysis_period_days=100000,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column='event_day',
    )
    kwargs.update(overrides)
    return mod.CustomerSegmentationComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frames, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component)] + [dg.SourceAsset(k) for k in frames],
        resources={"io_manager": _FrameIO(frames)},
        partition_key=partition_key,
        tags=tags or {},
    )


def _range_tags(start, end):
    return {
        "dagster/asset_partition_range_start": start,
        "dagster/asset_partition_range_end": end,
    }


def _stable(df):
    return df.drop(columns=[c for c in VOLATILE if c in df.columns]).reset_index(drop=True)


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


def test_single_partition_still_processes_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frames(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node(ASSET)
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frames = _frames()
    ranged = _run(component, frames, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node(ASSET)
    assert set(out["_partition_key"]) == set(DAYS)

    blocks = []
    for key in DAYS:
        single = _run(component, frames, partition_key=key).output_for_node(ASSET)
        block = out[out["_partition_key"] == key].drop(columns="_partition_key")
        pd.testing.assert_frame_equal(_stable(block), _stable(single))
        blocks.append(single)
    # each day's customers are different, so blocks must not be copies of each other
    assert not blocks[0]["customer_id"].equals(blocks[1]["customer_id"])


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frames(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node(ASSET)
    assert set(out["_partition_key"]) == {"2024-01-03", "2024-01-04"}


def test_range_without_filter_columns_runs_once(mod):
    """No partition_date_column -> the key never changes the computation, so a
    range must run it once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frames(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node(ASSET)
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frames()
    ).output_for_node(ASSET)
    pd.testing.assert_frame_equal(_stable(out), _stable(unpartitioned))


def test_rfm_scoring_survives_tied_frequencies(mod):
    """Regression: pd.qcut(..., duplicates='drop') with explicit labels raised
    'Bin labels must be one fewer than the number of bin edges' whenever many
    customers shared the same value (e.g. everyone bought once)."""
    component = _component(mod, partition_type=None, partition_start=None, partition_date_column=None)
    tx = pd.DataFrame([
        {"customer_id": i, "date": f"2024-01-{i % 28 + 1:02d}", "amount": 10.0 + i} for i in range(20)
    ])
    rfm = component._calculate_rfm_scores(tx)
    assert len(rfm) == 20
    assert set(rfm["F_score"]) <= {1, 2, 3, 4, 5}
