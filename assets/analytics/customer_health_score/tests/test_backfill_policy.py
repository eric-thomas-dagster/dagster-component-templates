"""backfill_policy tests for CustomerHealthScoreComponent.

The component filters its upstream DataFrame(s) to the current partition
(partition_date_column) before computing. A single_run backfill hands it a
partition RANGE, on which context.partition_key raises, so the computation runs
once per partition in the range and the frames are concatenated (adding a
_partition_key column).

Everything runs through real dg.materialize() with real DataFrames. The
upstream(s) are served by a tiny IOManager (dg.InMemoryIOManager semantics)
because they are external to the materialize call; nothing is mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FrameIO(dg.InMemoryIOManager):
    """InMemoryIOManager that serves the real frame for each external upstream asset."""

    def __init__(self, frames):
        super().__init__()
        self._frames = frames

    def load_input(self, context):
        key = context.asset_key.to_user_string()
        if key in self._frames:
            return self._frames[key]
        return super().load_input(context)


def _frames():
    cust, subs = [], []
    for k, (day, n) in enumerate((("2024-01-02", 3), ("2024-01-03", 4), ("2024-01-04", 5))):
        for i in range(n):
            cid = f"{day}-{i}"
            cust.append({"event_date": day, "customer_id": cid, "last_login_days": 1 + 12 * i + k,
                         "login_frequency": 25 - 4 * i - k, "feature_adoption_rate": max(0.05, 0.9 - 0.15 * i - 0.05 * k)})
            subs.append({"event_date": day, "customer_id": cid, "status": "active" if i % 2 == 0 else "past_due",
                         "days_subscribed": 100 + 50 * i, "mrr": 500 - 60 * i - 10 * k})
    return {"customers": pd.DataFrame(cust), "subs": pd.DataFrame(subs)}


def _component(mod, **overrides):
    kwargs = dict(asset_name="health", customer_data_asset_key="customers", subscription_data_asset_key="subs", partition_type="daily", partition_start="2024-01-02", partition_date_column="event_date")
    kwargs.update(overrides)
    return mod.CustomerHealthScoreComponent(**kwargs)


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


VOLATILE_COLUMNS = ('calculated_at',)  # run-to-run noise (timings), excluded from equality checks

def _assert_day_only(out, day):
    assert out["customer_id"].str.startswith(day).all()
    assert len(out) == {"2024-01-02": 3, "2024-01-03": 4, "2024-01-04": 5}[day]
    assert {"health_score", "risk_category"}.issubset(out.columns)


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
    out = result.output_for_node("health")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0
    _assert_day_only(out, "2024-01-03")


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frames = _frames()
    ranged = _run(component, frames, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node("health")
    assert set(out["_partition_key"]) == {"2024-01-02", "2024-01-03", "2024-01-04"}

    blocks = []
    for key in ("2024-01-02", "2024-01-03", "2024-01-04"):
        single = _run(component, frames, partition_key=key).output_for_node("health")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            block.drop(columns=list(VOLATILE_COLUMNS)), single.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS))
        )
        blocks.append(block.drop(columns=list(VOLATILE_COLUMNS)))
    # per-day data was built to differ, so the blocks must not all be the same
    assert len({b.to_csv(index=False) for b in blocks}) > 1


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frames(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node("health")
    assert set(out["_partition_key"]) == {"2024-01-03", "2024-01-04"}


def test_range_without_filter_columns_runs_once(mod):
    """No partition_date_column -> the key never changes the computation, so a
    range must run once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frames(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("health")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frames()
    ).output_for_node("health")
    pd.testing.assert_frame_equal(
        out.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS)),
        unpartitioned.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS)),
    )


# --- scoring_method="ml": the second wrapped asset ---------------------------

def _ml_frames():
    rows = []
    for k, day in enumerate(("2024-01-02", "2024-01-03", "2024-01-04")):
        for i in range(10):
            rows.append({
                "event_date": day, "customer_id": f"{day}-{i}", "login_frequency": float(i + k), "mrr": float((i * (k + 3)) % 11),
                "churned": int(i >= 5) if k != 1 else int(i % 3 == 0),
            })
    return {"labeled": pd.DataFrame(rows)}


def _ml_component(mod, **overrides):
    return _component(
        mod, asset_name="health", scoring_method="ml", customer_data_asset_key=None, subscription_data_asset_key=None, upstream_asset_key="labeled", target_column="churned", feature_columns=["login_frequency", "mrr"], **overrides,
    )


def test_ml_asset_backfill_policy_wired(mod):
    assert _asset(_ml_component(mod, backfill_policy="single_run")).backfill_policy == dg.BackfillPolicy.single_run()
    assert _asset(_ml_component(mod)).backfill_policy is None


def test_ml_asset_single_run_range_runs_per_partition(mod):
    component = _ml_component(mod, backfill_policy="single_run")
    frames = _ml_frames()
    out = _run(component, frames, tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("health")
    assert set(out["_partition_key"]) == {"2024-01-02", "2024-01-03", "2024-01-04"}
    assert "predicted_class" in out.columns
    for key in ("2024-01-02", "2024-01-03", "2024-01-04"):
        single = _run(component, frames, partition_key=key).output_for_node("health")
        assert "_partition_key" not in single.columns
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        assert block["customer_id"].str.startswith(key).all() and len(block) == 10
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))
