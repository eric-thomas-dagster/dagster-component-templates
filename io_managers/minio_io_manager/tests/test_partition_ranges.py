"""Partition-range (single_run backfill) behavior of MinIODeltaIOManager.

Real dg.materialize() runs against REAL Delta tables (deltalake) on the local
filesystem.  The only substitution is where the table lives: _table_path()
points at a tmp directory instead of s3://bucket/... and storage options are
empty.  Partition values / predicate building / read filtering is the real
manager code.
"""
import pandas as pd
import pytest
import dagster as dg
from deltalake import DeltaTable

from .conftest import load_component_module

mod = load_component_module()

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-03",
}
PARTS = dg.DailyPartitionsDefinition(start_date="2024-01-01")


class LocalDeltaManager(mod.MinIODeltaIOManager):
    root: str = ""

    def _get_storage_options(self):
        return {}

    def _table_path(self, context):
        return f"{self.root}/" + "/".join(context.asset_key.path)


@pytest.fixture
def mgr(tmp_path):
    return LocalDeltaManager(bucket="b", root=str(tmp_path))


def _rows(mgr, name="events"):
    return DeltaTable(f"{mgr.root}/{name}").to_pandas()


def _chain_text(exc):
    out, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        out.append(str(exc))
        exc = exc.__cause__ or exc.__context__
    return " | ".join(out)


def _day(k, n=2, tag=0):
    return pd.DataFrame({"v": [f"{k}-{i}-{tag}" for i in range(n)]})


def make_range_asset(payload_fn):
    @dg.asset(name="events", partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def events(context: dg.AssetExecutionContext):
        return payload_fn(list(context.partition_keys))

    return events


@dg.asset(name="events", partitions_def=PARTS)
def events_single(context: dg.AssetExecutionContext):
    return _day(context.partition_key, tag=9)


def test_range_write_mapping_writes_every_key(mgr):
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    r = dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert r.success
    df = _rows(mgr)
    assert sorted(df["partition_key"].unique()) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert len(df) == 6
    md = r.get_asset_materialization_events()[0].materialization.metadata
    assert md["partition_key"].value == "2024-01-01...2024-01-03"
    assert md["row_count"].value == 6


def test_range_write_preserves_other_partitions_and_is_idempotent(mgr):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    for _ in range(2):
        assert dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS).success
    df = _rows(mgr)
    assert len(df[df.partition_key == "2024-01-04"]) == 2  # untouched
    assert len(df[df.partition_key == "2024-01-02"]) == 2  # no duplicates after rerun
    assert len(df) == 8


def test_range_rewrite_clears_stale_rows_for_keys_without_new_data(mgr):
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    b = make_range_asset(lambda keys: {k: _day(k, n=0) if k == "2024-01-02" else _day(k) for k in keys})
    assert dg.materialize([b], resources={"io_manager": mgr}, tags=RANGE_TAGS).success
    df = _rows(mgr)
    assert "2024-01-02" not in set(df.partition_key)
    assert len(df) == 4


def test_range_write_dataframe_with_partition_column(mgr):
    def payload(keys):
        return pd.concat([_day(k).assign(partition_key=k) for k in keys], ignore_index=True)

    r = dg.materialize([make_range_asset(payload)], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert r.success
    assert len(_rows(mgr)) == 6


def test_range_write_dataframe_with_foreign_key_errors_and_changes_nothing(mgr):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    bad = make_range_asset(lambda keys: pd.DataFrame({"v": ["x"], "partition_key": ["2030-01-01"]}))
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "outside the materialized partition range" in _chain_text(ei.value)
    assert len(_rows(mgr)) == 2


def test_range_write_plain_dataframe_without_column_errors(mgr):
    bad = make_range_asset(lambda keys: pd.DataFrame({"v": [1]}))
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "has no 'partition_key' column" in _chain_text(ei.value)


def test_range_write_mapping_missing_key_errors(mgr):
    bad = make_range_asset(lambda keys: {keys[0]: _day(keys[0])})
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "must equal the materialized partition range" in _chain_text(ei.value)


def test_range_write_non_frame_non_mapping_errors(mgr):
    bad = make_range_asset(lambda keys: [1, 2, 3])
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "dict {partition_key: DataFrame}" in _chain_text(ei.value)


def test_range_load_returns_one_dataframe_of_only_range_rows(mgr):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    seen = {}

    @dg.asset(name="events", partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def events(context: dg.AssetExecutionContext):
        return {k: _day(k) for k in context.partition_keys}

    @dg.asset(partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def summary(context: dg.AssetExecutionContext, events: pd.DataFrame):
        seen["df"] = events
        return {k: pd.DataFrame({"c": [int((events.partition_key == k).sum())]}) for k in context.partition_keys}

    r = dg.materialize([events, summary], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert r.success
    got = seen["df"]
    assert isinstance(got, pd.DataFrame)
    assert sorted(got.partition_key.unique()) == ["2024-01-01", "2024-01-02", "2024-01-03"]  # not 01-04
    assert len(got) == 6
    s = _rows(mgr, "summary")
    assert sorted(s.partition_key) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert set(s.c) == {2}


def test_single_partition_regression(mgr):
    for k in ("2024-01-01", "2024-01-02", "2024-01-02"):
        assert dg.materialize([events_single], resources={"io_manager": mgr}, partition_key=k).success
    df = _rows(mgr)
    assert len(df) == 4
    assert sorted(df.partition_key.unique()) == ["2024-01-01", "2024-01-02"]


def test_single_partition_load_regression(mgr):
    seen = {}

    @dg.asset(partitions_def=PARTS)
    def reader(events: pd.DataFrame):
        seen["df"] = events
        return pd.DataFrame({"c": [len(events)]})

    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-01")
    r = dg.materialize([events_single, reader], resources={"io_manager": mgr}, partition_key="2024-01-02")
    assert r.success
    assert set(seen["df"].partition_key) == {"2024-01-02"}


def test_unpartitioned_regression(mgr):
    @dg.asset(name="plain")
    def plain():
        return pd.DataFrame({"a": [1, 2, 3]})

    assert dg.materialize([plain], resources={"io_manager": mgr}).success
    assert dg.materialize([plain], resources={"io_manager": mgr}).success
    assert list(_rows(mgr, "plain").a) == [1, 2, 3]
