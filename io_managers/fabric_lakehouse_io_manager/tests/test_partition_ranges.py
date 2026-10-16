"""Only the OneLake network hop is faked: abfss://.../Tables/<name> is redirected to a local
directory and the REAL deltalake writer/reader runs against it. Path/table-name building is real."""
import dagster as dg
import deltalake
import pandas as pd
import pytest

DAYS = ["2024-01-01", "2024-01-02", "2024-01-03"]
RANGE_TAGS = {
    "dagster/asset_partition_range_start": DAYS[0],
    "dagster/asset_partition_range_end": DAYS[-1],
}
PARTS = dg.DailyPartitionsDefinition("2024-01-01", end_offset=3)
REAL_TABLE = deltalake.DeltaTable  # captured before monkeypatching
PREFIX = "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/"


@pytest.fixture
def lake(tmp_path, monkeypatch):
    captured = []
    real_write, real_table = deltalake.write_deltalake, deltalake.DeltaTable

    def local(path):
        assert path.startswith(PREFIX), path
        captured.append(path)
        return str(tmp_path / path[len(PREFIX):])

    monkeypatch.setattr(deltalake, "write_deltalake", lambda path, df, **kw: real_write(local(path), df, mode=kw["mode"]))
    monkeypatch.setattr(deltalake, "DeltaTable", lambda path, **kw: real_table(local(path)))
    return tmp_path, captured


@pytest.fixture
def manager(mod):
    return mod.FabricLakehouseIOManager(workspace_id="ws-guid", lakehouse_name="lh")


def _frame(day, mult=1):
    return pd.DataFrame({"day": [day, day], "v": [1 * mult, 2 * mult]})


def _chain(exc):
    out = []
    while exc is not None:
        out.append(str(exc))
        exc = exc.__cause__
    return out


def _table(root, name):
    return REAL_TABLE(str(root / name)).to_pandas()


def _assets():
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def my_upstream(context):
        return {k: _frame(k) for k in context.partition_keys}

    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def my_downstream(context, my_upstream):
        assert sorted(my_upstream) == sorted(context.partition_keys)
        return {k: df.assign(v=df["v"] * 10) for k, df in my_upstream.items()}

    return my_upstream, my_downstream


def test_range_write_and_load_each_key(lake, manager):
    root, captured = lake
    up, down = _assets()
    res = dg.materialize([up, down], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert res.success
    for d in DAYS:
        t = d.replace("-", "_")
        pd.testing.assert_frame_equal(_table(root, f"my_upstream__{t}"), _frame(d), check_dtype=False)
        pd.testing.assert_frame_equal(_table(root, f"my_downstream__{t}"), _frame(d, 10), check_dtype=False)
        assert PREFIX + f"my_upstream__{t}" in captured
    assert not (root / "my_upstream").exists()  # range not treated as unpartitioned
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert meta["partition_key"].value == "2024-01-01...2024-01-03"
    assert meta["row_count"].value == 6


def test_range_non_mapping_output_is_a_clear_error(lake, manager):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return _frame("x")

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert any("{partition_key: DataFrame}" in m for m in _chain(ei.value))


def test_range_wrong_keys_is_a_clear_error(lake, manager):
    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def bad():
        return {DAYS[0]: _frame(DAYS[0])}

    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=RANGE_TAGS, resources={"io_manager": manager})
    assert any("Missing" in m and "2024-01-02" in m for m in _chain(ei.value))


def test_single_partition_unchanged(lake, manager):
    root, captured = lake

    @dg.asset(partitions_def=PARTS)
    def my_one(context):
        return _frame(context.partition_key)

    @dg.asset(partitions_def=PARTS)
    def my_two(my_one):
        return my_one.assign(v=my_one["v"] + 1)

    res = dg.materialize([my_one, my_two], partition_key=DAYS[1], resources={"io_manager": manager})
    assert res.success
    assert captured[0] == PREFIX + "my_one__2024_01_02"
    assert _table(root, "my_two__2024_01_02")["v"].tolist() == [2, 3]
    meta = res.get_asset_materialization_events()[0].materialization.metadata
    assert "table" in meta and "onelake_path" in meta


def test_unpartitioned_unchanged(lake, manager):
    root, _ = lake

    @dg.asset
    def my_plain():
        return _frame("p")

    @dg.asset
    def my_after(my_plain):
        return my_plain.assign(v=my_plain["v"] * 3)

    res = dg.materialize([my_plain, my_after], resources={"io_manager": manager})
    assert res.success
    assert _table(root, "my_after")["v"].tolist() == [3, 6]
