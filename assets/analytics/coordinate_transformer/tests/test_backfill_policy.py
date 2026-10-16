"""backfill_policy tests for CoordinateTransformerComponent.

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

try:
    import pyproj  # noqa: F401
    _HAS_PYPROJ = True
except ImportError:
    _HAS_PYPROJ = False

requires_pyproj = pytest.mark.skipif(not _HAS_PYPROJ, reason="coordinate_transformer needs pyproj (pip install pyproj)")


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
    rows = []
    for day, n in (("2024-01-02", 2), ("2024-01-03", 3), ("2024-01-04", 4)):
        for i in range(n):
            rows.append({"event_date": day, "id": f"{day}-{i}", "latitude": 40.0 + i, "longitude": -74.0 + i + int(day[-1])})
    return {"events": pd.DataFrame(rows)}


def _component(mod, **overrides):
    kwargs = dict(asset_name="events_web_mercator", upstream_asset_key="events", x_column="longitude", y_column="latitude", source_crs="EPSG:4326", target_crs="EPSG:3857", output_x_column="x_mercator", output_y_column="y_mercator", partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date")
    kwargs.update(overrides)
    return mod.CoordinateTransformerComponent(**kwargs)


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


VOLATILE_COLUMNS = ()  # run-to-run noise (timings), excluded from equality checks

def _assert_day_only(out, day):
    assert set(out["event_date"]) == {day}
    assert len(out) == {"2024-01-02": 2, "2024-01-03": 3, "2024-01-04": 4}[day]
    # EPSG:3857 x for lng -74 is about -8.2e6 (metres) -- really reprojected, not degrees
    assert (out["x_mercator"].abs() > 1e6).all()


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


@requires_pyproj
def test_single_partition_still_processes_only_that_days_rows(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frames(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("events_web_mercator")
    assert "_partition_key" not in out.columns  # ordinary run: output shape unchanged
    assert len(out) > 0
    _assert_day_only(out, "2024-01-03")


@requires_pyproj
def test_single_run_range_runs_per_partition_and_concatenates(mod):
    """RANGE logic must actually run: the output carries one block per
    partition in the range, each identical to that partition's own run."""
    component = _component(mod, backfill_policy="single_run")
    frames = _frames()
    ranged = _run(component, frames, tags=_range_tags("2024-01-02", "2024-01-04"))
    assert ranged.success
    out = ranged.output_for_node("events_web_mercator")
    assert set(out["_partition_key"]) == {"2024-01-02", "2024-01-03", "2024-01-04"}

    blocks = []
    for key in ("2024-01-02", "2024-01-03", "2024-01-04"):
        single = _run(component, frames, partition_key=key).output_for_node("events_web_mercator")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            block.drop(columns=list(VOLATILE_COLUMNS)), single.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS))
        )
        blocks.append(block.drop(columns=list(VOLATILE_COLUMNS)))
    # per-day data was built to differ, so the blocks must not all be the same
    assert len({b.to_csv(index=False) for b in blocks}) > 1


@requires_pyproj
def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frames(), tags=_range_tags("2024-01-03", "2024-01-04")).output_for_node("events_web_mercator")
    assert set(out["_partition_key"]) == {"2024-01-03", "2024-01-04"}


@requires_pyproj
def test_range_without_filter_columns_runs_once(mod):
    """No partition_date_column -> the key never changes the computation, so a
    range must run once (not N identical copies)."""
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frames(), tags=_range_tags("2024-01-02", "2024-01-04")).output_for_node("events_web_mercator")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frames()
    ).output_for_node("events_web_mercator")
    pd.testing.assert_frame_equal(
        out.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS)),
        unpartitioned.reset_index(drop=True).drop(columns=list(VOLATILE_COLUMNS)),
    )
