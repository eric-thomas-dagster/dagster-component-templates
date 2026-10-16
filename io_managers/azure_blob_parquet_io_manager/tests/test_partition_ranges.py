import io
import types

import dagster as dg
import pandas as pd
import pytest
from .conftest import *  # noqa: F401,F403  (fake client + fixtures)

DAYS = ["2024-01-01", "2024-01-02", "2024-01-03"]
RANGE_TAGS = {
    "dagster/asset_partition_range_start": DAYS[0],
    "dagster/asset_partition_range_end": DAYS[-1],
}
PARTS = dg.DailyPartitionsDefinition("2024-01-01", end_offset=3)


def _frame(day, mult=1):
    return pd.DataFrame({"day": [day, day], "v": [1 * mult, 2 * mult]})


def _assets():
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def my_upstream(context):
        return {k: _frame(k) for k in context.partition_keys}

    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def my_downstream(context, my_upstream):
        assert isinstance(my_upstream, dict)
        assert sorted(my_upstream) == sorted(context.partition_keys)
        return {k: df.assign(v=df["v"] * 10) for k, df in my_upstream.items()}

    return my_upstream, my_downstream


def _chain(exc):
    out = []
    while exc is not None:
        out.append(str(exc))
        exc = exc.__cause__
    return out


def test_range_write_and_load_each_key(store, manager):
    up, down = _assets()
    res = dg.materialize([up, down], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert res.success
    assert sorted(store) == sorted(
        [full_key(f"my_upstream/{d}") for d in DAYS] + [full_key(f"my_downstream/{d}") for d in DAYS]
    )
    for d in DAYS:
        pd.testing.assert_frame_equal(decode(store[full_key(f"my_upstream/{d}")]), _frame(d), check_dtype=False)
        pd.testing.assert_frame_equal(decode(store[full_key(f"my_downstream/{d}")]), _frame(d, 10), check_dtype=False)
    assert full_key("my_upstream") not in store  # range must NOT be treated as unpartitioned
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert meta["partition_key"].value == "2024-01-01...2024-01-03"
    assert meta["row_count"].value == 6


def test_range_non_mapping_output_is_a_clear_error(store, manager):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return _frame("x")

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert any("{partition_key: DataFrame}" in m for m in _chain(ei.value))
    assert store == {}


def test_range_wrong_keys_is_a_clear_error(store, manager):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return {DAYS[0]: _frame(DAYS[0])}

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert any("Missing" in m and "2024-01-02" in m for m in _chain(ei.value))
    assert store == {}


def test_single_partition_unchanged(store, manager):
    @dg.asset(partitions_def=PARTS)
    def my_one(context):
        return _frame(context.partition_key)

    @dg.asset(partitions_def=PARTS)
    def my_two(my_one):
        return my_one.assign(v=my_one["v"] + 1)

    res = dg.materialize([my_one, my_two], partition_key=DAYS[1], resources={"io_manager": manager})
    assert res.success
    assert sorted(store) == [full_key("my_one/2024-01-02"), full_key("my_two/2024-01-02")]
    assert decode(store[full_key("my_two/2024-01-02")])["v"].tolist() == [2, 3]


def test_unpartitioned_unchanged(store, manager):
    @dg.asset
    def my_plain():
        return _frame("p")

    @dg.asset
    def my_after(my_plain):
        return my_plain.assign(v=my_plain["v"] * 3)

    res = dg.materialize([my_plain, my_after], resources={"io_manager": manager})
    assert res.success
    assert sorted(store) == [full_key("my_after"), full_key("my_plain")]
    assert decode(store[full_key("my_after")])["v"].tolist() == [3, 6]
