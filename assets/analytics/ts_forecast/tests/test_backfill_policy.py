"""backfill_policy tests for TsForecastComponent.

Time-series models need ONE series, so the natural partition is a static series key filtered through
partition_static_column. A single_run backfill hands the component a partition RANGE, so a forecast is
fitted once per series in the range and the forecast frames are concatenated. Needs statsmodels.

Everything runs through real dg.materialize() with real data, the upstream served by a tiny IOManager (the upstream asset is external to the materialize call).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("statsmodels")

UPSTREAM = "upstream_data"

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



def _frame(series=("a", "b", "c")):
    import numpy as np

    rows = []
    months = pd.date_range("2020-01-01", periods=36, freq="MS")
    for idx, s in enumerate(("a", "b", "c")):
        if s not in series:
            continue
        rng = np.random.RandomState(idx + 1)
        for t, month in enumerate(months):
            value = 100 + 40 * idx + (2 + idx) * t + 8 * np.sin(2 * np.pi * t / 12) + rng.normal(0, 1.5)
            rows.append({"series": s, "month": month, "revenue": float(value)})
    return pd.DataFrame(rows)


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="ts_fc",
        upstream_asset_key=UPSTREAM,
        date_column="month",
        value_column="revenue",
        forecast_periods=3,
        model="ets",
        ets_trend="add",
        ets_seasonal="add",
        ets_seasonal_periods=12,
        output_mode="forecast_only",
        partition_type="static",
        partition_values="a,b,c",
        partition_static_column="series",
    )
    kwargs.update(overrides)
    return mod.TsForecastComponent(**kwargs)


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
    result = _run(_component(mod, backfill_policy="single_run"), _frame(), partition_key="b")
    assert result.success
    out = result.output_for_node("ts_fc")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frame = _frame()
    ranged = _run(component, frame, tags=_range_tags("a", "c"))
    assert ranged.success
    out = ranged.output_for_node("ts_fc")
    assert set(out["_partition_key"]) == {'b', 'a', 'c'}

    for key in ('a', 'b', 'c'):
        single = _run(component, frame, partition_key=key).output_for_node("ts_fc")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        assert len(block) > 0
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frame(), tags=_range_tags("b", "c")).output_for_node("ts_fc")
    assert set(out["_partition_key"]) == {'b', 'c'}


def test_range_without_filter_columns_runs_once(mod):
    """No partition filter column -> the key never changes the computation, so a
    range must run the analysis once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None, partition_static_column=None)
    out = _run(component, _frame(series=('a',)), tags=_range_tags("a", "c")).output_for_node("ts_fc")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(_unpartitioned(mod, partition_date_column=None, partition_static_column=None), _frame(series=('a',))).output_for_node("ts_fc")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))


def test_forecasts_differ_per_series(mod):
    out = _run(_component(mod, backfill_policy="single_run"), _frame(),
               tags=_range_tags("a", "c")).output_for_node("ts_fc")
    assert len(out) == 9 and out["is_forecast"].all()
    firsts = out.groupby("_partition_key")["revenue"].first()
    assert firsts.nunique() == 3
    assert firsts["a"] < firsts["b"] < firsts["c"]  # series baselines are 100/140/180


def test_append_mode_keeps_history_per_partition(mod):
    out = _run(_component(mod, backfill_policy="single_run", output_mode="append"), _frame(),
               tags=_range_tags("a", "b")).output_for_node("ts_fc")
    for key, block in out.groupby("_partition_key"):
        assert (~block["is_forecast"]).sum() == 36 and block["is_forecast"].sum() == 3
