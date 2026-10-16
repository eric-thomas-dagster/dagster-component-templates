import dagster as dg
import pandas as pd
import pytest

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
    def upstream(context):
        return {k: _frame(k) for k in context.partition_keys}

    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def downstream(context, upstream):
        assert isinstance(upstream, dict)
        assert sorted(upstream) == sorted(context.partition_keys)
        return {k: df.assign(v=df["v"] * 10) for k, df in upstream.items()}

    return upstream, downstream


def _chain(exc):
    out = []
    while exc is not None:
        out.append(str(exc))
        exc = exc.__cause__
    return out


def _same(df, expected):
    pd.testing.assert_frame_equal(
        df.reset_index(drop=True)[list(expected.columns)], expected, check_dtype=False
    )


def test_range_write_and_load_each_key(be):
    up, down = _assets()
    res = dg.materialize([up, down], tags=RANGE_TAGS, resources={"io_manager": be.manager})
    assert res.success
    for d in DAYS:
        _same(be.read("upstream", d), _frame(d))
        _same(be.read("downstream", d), _frame(d, 10))
    # the range must NOT be treated as unpartitioned
    assert not be.has("upstream", None)
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert meta["partition_key"].value == "2024-01-01...2024-01-03"
    assert meta["row_count"].value == 6


def test_range_subrange_writes_only_those_keys(be):
    up, _ = _assets()
    tags = {**RANGE_TAGS, "dagster/asset_partition_range_end": DAYS[1]}
    res = dg.materialize([up], tags=tags, resources={"io_manager": be.manager})
    assert res.success
    assert be.has("upstream", DAYS[0]) and be.has("upstream", DAYS[1])
    assert not be.has("upstream", DAYS[2])


def test_range_does_not_touch_other_partitions(be):
    up, _ = _assets()

    @dg.asset(partitions_def=PARTS, name="upstream")
    def single(context):
        return _frame(context.partition_key)

    res = dg.materialize([single], partition_key="2024-01-04", resources={"io_manager": be.manager})
    assert res.success
    res = dg.materialize([up], tags=RANGE_TAGS, resources={"io_manager": be.manager})
    assert res.success
    _same(be.read("upstream", "2024-01-04"), _frame("2024-01-04"))
    assert be.has("upstream", DAYS[0])


def test_range_non_mapping_output_is_a_clear_error(be):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return _frame("x")

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": be.manager})
    assert any("{partition_key: DataFrame}" in m for m in _chain(ei.value)), _chain(ei.value)
    assert not be.has("bad", None)


def test_range_wrong_keys_is_a_clear_error(be):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return {DAYS[0]: _frame(DAYS[0])}

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": be.manager})
    assert any("Missing" in m and "2024-01-02" in m for m in _chain(ei.value)), _chain(ei.value)
    assert not be.has("bad", DAYS[0])  # nothing written when validation fails


def test_range_non_dataframe_value_writes_nothing(be):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return {DAYS[0]: _frame(DAYS[0]), DAYS[1]: "oops", DAYS[2]: _frame(DAYS[2])}

    with pytest.raises(Exception):
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": be.manager})
    assert not be.has("bad", DAYS[0])


def test_single_partition_unchanged(be):
    @dg.asset(partitions_def=PARTS)
    def one(context):
        return _frame(context.partition_key)

    @dg.asset(partitions_def=PARTS)
    def two(one):
        return one.assign(v=one["v"] + 1)

    res = dg.materialize([one, two], partition_key=DAYS[1], resources={"io_manager": be.manager})
    assert res.success
    _same(be.read("one", DAYS[1]), _frame(DAYS[1]))
    assert be.read("two", DAYS[1])["v"].tolist() == [2, 3]
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert meta["row_count"].value == 2
    assert not be.has("one", None)


def test_unpartitioned_unchanged(be):
    @dg.asset
    def plain():
        return _frame("p")

    @dg.asset
    def after(plain):
        return plain.assign(v=plain["v"] * 3)

    res = dg.materialize([plain, after], resources={"io_manager": be.manager})
    assert res.success
    assert be.has("plain", None)
    assert be.read("after", None)["v"].tolist() == [3, 6]
