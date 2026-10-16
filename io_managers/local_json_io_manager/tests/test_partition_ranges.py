import dagster as dg
import pandas as pd
import pytest

DAYS = ["2024-01-01", "2024-01-02", "2024-01-03"]
RANGE_TAGS = {
    "dagster/asset_partition_range_start": DAYS[0],
    "dagster/asset_partition_range_end": DAYS[-1],
}
PARTS = dg.DailyPartitionsDefinition("2024-01-01", end_offset=3)


def _read(path):
    return pd.read_json(path, orient="records", lines=True)


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


def test_range_write_and_load_each_key(tmp_path, mod):
    up, down = _assets()
    res = dg.materialize([up, down], tags=RANGE_TAGS, resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    assert res.success
    for d in DAYS:
        pd.testing.assert_frame_equal(_read(tmp_path / "upstream" / f"{d}.jsonl"), _frame(d), check_dtype=False)
        pd.testing.assert_frame_equal(_read(tmp_path / "downstream" / f"{d}.jsonl"), _frame(d, 10), check_dtype=False)
    # the range must NOT be treated as unpartitioned
    assert not (tmp_path / "upstream.jsonl").exists()
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert meta["partition_key"].value == "2024-01-01...2024-01-03"
    assert meta["row_count"].value == 6


def test_range_partial_subrange_writes_only_those_keys(tmp_path, mod):
    up, _ = _assets()
    tags = {**RANGE_TAGS, "dagster/asset_partition_range_end": DAYS[1]}
    res = dg.materialize([up], tags=tags, resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    assert res.success
    assert (tmp_path / "upstream" / "2024-01-02.jsonl").exists()
    assert not (tmp_path / "upstream" / "2024-01-03.jsonl").exists()


def test_range_non_mapping_output_is_a_clear_error(tmp_path, mod):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return _frame("x")

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    chain, e = [], ei.value
    while e is not None:
        chain.append(str(e))
        e = e.__cause__
    assert any("{partition_key: DataFrame}" in m for m in chain), chain


def test_range_wrong_keys_is_a_clear_error(tmp_path, mod):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return {DAYS[0]: _frame(DAYS[0])}

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    chain, e = [], ei.value
    while e is not None:
        chain.append(str(e))
        e = e.__cause__
    assert any("Missing" in m and "2024-01-02" in m for m in chain), chain


def test_single_partition_unchanged(tmp_path, mod):
    @dg.asset(partitions_def=PARTS)
    def one(context):
        return _frame(context.partition_key)

    @dg.asset(partitions_def=PARTS)
    def two(one):
        return one.assign(v=one["v"] + 1)

    res = dg.materialize([one, two], partition_key=DAYS[1], resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    assert res.success
    pd.testing.assert_frame_equal(_read(tmp_path / "one" / "2024-01-02.jsonl"), _frame(DAYS[1]), check_dtype=False)
    assert _read(tmp_path / "two" / "2024-01-02.jsonl")["v"].tolist() == [2, 3]
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert "path" in meta


def test_unpartitioned_unchanged(tmp_path, mod):
    @dg.asset
    def plain():
        return _frame("p")

    @dg.asset
    def after(plain):
        return plain.assign(v=plain["v"] * 3)

    res = dg.materialize([plain, after], resources={"io_manager": mod.LocalJsonIOManager(base_dir=str(tmp_path))})
    assert res.success
    assert (tmp_path / "plain.jsonl").exists()
    assert _read(tmp_path / "after.jsonl")["v"].tolist() == [3, 6]
