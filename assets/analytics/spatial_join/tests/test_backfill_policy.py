"""backfill_policy tests for SpatialJoinComponent.

The points upstream is filtered to the current partition (partition_date_column)
before the spatial join; the regions upstream is not partitioned. A single_run
backfill hands the asset a partition RANGE, so the join runs once per partition
and the frames are concatenated with a '_partition_key' column.
Real geopandas/shapely and real dg.materialize(); nothing is mocked. Both
upstreams (external to the materialize call) are served by a tiny IOManager.
Skipped when geopandas/shapely are not installed.
"""
import json

import dagster as dg
import pandas as pd
import pytest

pytest.importorskip("geopandas")
pytest.importorskip("shapely")

from .conftest import load_component_module  # noqa: E402

POINTS = "points"
REGIONS = "regions"
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


def _square(x0, y0, size):
    ring = [[x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]
    return json.dumps({"type": "Polygon", "coordinates": [ring]})


def _frames():
    regions = pd.DataFrame([
        {"region_name": "west", "geometry": _square(0, 0, 10)},
        {"region_name": "east", "geometry": _square(20, 0, 10)},
    ])
    rows = []
    for d, day in enumerate(DAYS):
        for i in range(d + 2):
            rows.append({"seen_on": day, "pid": f"{day}-w{i}", "latitude": 1.0 + i, "longitude": 1.0 + d})
        rows.append({"seen_on": day, "pid": f"{day}-e", "latitude": 5.0, "longitude": 25.0 + d})
    return {POINTS: pd.DataFrame(rows), REGIONS: regions}


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="joined",
        upstream_asset_key=POINTS,
        regions_asset_key=REGIONS,
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="seen_on",
    )
    kwargs.update(overrides)
    return mod.SpatialJoinComponent(**kwargs)


def _asset(component):
    return component.build_defs(None).assets[0]


def _run(component, frames, partition_key=None, tags=None):
    return dg.materialize(
        [_asset(component), dg.SourceAsset(POINTS), dg.SourceAsset(REGIONS)],
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


def test_single_partition_still_joins_only_that_days_points(mod):
    result = _run(_component(mod, backfill_policy="single_run"), _frames(), partition_key="2024-01-03")
    assert result.success
    out = result.output_for_node("joined")
    assert "_partition_key" not in out.columns
    assert set(out["seen_on"]) == {"2024-01-03"}
    assert len(out) == 4  # 3 west points + 1 east point
    assert set(out["region_name"]) == {"west", "east"}


def test_single_run_range_runs_per_partition_and_concatenates(mod):
    component = _component(mod, backfill_policy="single_run")
    frames = _frames()
    ranged = _run(component, frames, tags=_range_tags(DAYS[0], DAYS[-1]))
    assert ranged.success
    out = ranged.output_for_node("joined")
    assert set(out["_partition_key"]) == set(DAYS)
    for key in DAYS:
        single = _run(component, frames, partition_key=key).output_for_node("joined")
        block = out[out["_partition_key"] == key].drop(columns="_partition_key").reset_index(drop=True)
        pd.testing.assert_frame_equal(block, single.reset_index(drop=True))


def test_single_run_range_excludes_partitions_outside_the_range(mod):
    component = _component(mod, backfill_policy="single_run")
    out = _run(component, _frames(), tags=_range_tags(DAYS[1], DAYS[2])).output_for_node("joined")
    assert set(out["_partition_key"]) == {DAYS[1], DAYS[2]}
    assert set(out["seen_on"]) == {DAYS[1], DAYS[2]}


def test_range_without_filter_columns_runs_once(mod):
    component = _component(mod, backfill_policy="single_run", partition_date_column=None)
    out = _run(component, _frames(), tags=_range_tags(DAYS[0], DAYS[-1])).output_for_node("joined")
    assert "_partition_key" not in out.columns
    unpartitioned = _run(
        _component(mod, partition_type=None, partition_start=None, partition_date_column=None), _frames()
    ).output_for_node("joined")
    pd.testing.assert_frame_equal(out.reset_index(drop=True), unpartitioned.reset_index(drop=True))
    assert len(out) == 3 + 4 + 5  # every day's points (west + east), joined once
