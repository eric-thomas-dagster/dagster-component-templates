"""Partition-range (single_run backfill) behavior of AthenaIOManager.

Real dg.materialize() runs.  awswrangler (a network client, not installed here)
is replaced by a fake module whose to_parquet() implements Athena's
``overwrite_partitions`` / ``overwrite`` semantics over a real sqlite table and
whose read_sql_query() really executes the SQL the manager built (only the
"db"."table" qualifier is stripped for sqlite).  Every call is captured.
"""
import re
import sqlite3
import sys
import types

import pandas as pd
import pytest
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-03",
}
PARTS = dg.DailyPartitionsDefinition(start_date="2024-01-01")
CALLS = []


@pytest.fixture(autouse=True)
def fake_wr(monkeypatch):
    CALLS.clear()
    db = sqlite3.connect(":memory:", check_same_thread=False)
    wr = types.ModuleType("awswrangler")
    wr.catalog = types.SimpleNamespace(create_database=lambda **kw: None)

    def to_parquet(df, path, dataset, database, table, mode, partition_cols=None, **kw):
        CALLS.append({"mode": mode, "partition_cols": partition_cols, "path": path, "rows": len(df)})
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone()
        if mode == "overwrite_partitions" and exists:
            col = partition_cols[0]
            vals = sorted(df[col].unique())
            db.execute(f'DELETE FROM "{table}" WHERE "{col}" IN ({",".join("?" * len(vals))})', vals)
            df.to_sql(table, db, if_exists="append", index=False)
        else:
            df.to_sql(table, db, if_exists="replace", index=False)

    def read_sql_query(query, database, s3_output, **kw):
        CALLS.append({"query": query})
        return pd.read_sql(re.sub(r'"%s"\.' % database, "", query), db)

    wr.s3 = types.SimpleNamespace(to_parquet=to_parquet)
    wr.athena = types.SimpleNamespace(read_sql_query=read_sql_query)
    monkeypatch.setitem(sys.modules, "awswrangler", wr)
    return db


@pytest.fixture
def db(fake_wr):
    return fake_wr


@pytest.fixture
def mgr():
    return mod.AthenaIOManager(database="mydb", s3_staging_dir="s3://b/q/", s3_output_location="s3://b/out/")


def _rows(db, name="events"):
    return pd.read_sql(f"SELECT * FROM {name}", db)


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


def test_range_write_mapping_writes_every_key(mgr, db):
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    r = dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert r.success
    df = _rows(db)
    assert sorted(df["partition_key"].unique()) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert len(df) == 6
    assert [c["mode"] for c in CALLS] == ["overwrite_partitions"]  # one write for the whole range
    md = r.get_asset_materialization_events()[0].materialization.metadata
    assert md["partition_key"].value == "2024-01-01...2024-01-03"
    assert md["row_count"].value == 6


def test_range_write_preserves_other_partitions_and_is_idempotent(mgr, db):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    for _ in range(2):
        assert dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS).success
    df = _rows(db)
    assert len(df[df.partition_key == "2024-01-04"]) == 2  # untouched
    assert len(df[df.partition_key == "2024-01-02"]) == 2  # no duplicates after rerun
    assert len(df) == 8


def test_range_write_dataframe_with_partition_column(mgr, db):
    def payload(keys):
        return pd.concat([_day(k).assign(partition_key=k) for k in keys], ignore_index=True)

    r = dg.materialize([make_range_asset(payload)], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert r.success
    assert len(_rows(db)) == 6


def test_range_write_dataframe_with_foreign_key_errors_and_changes_nothing(mgr, db):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    bad = make_range_asset(lambda keys: pd.DataFrame({"v": ["x"], "partition_key": ["2030-01-01"]}))
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "outside the materialized partition range" in _chain_text(ei.value)
    assert len(_rows(db)) == 2


def test_range_write_plain_dataframe_without_column_errors(mgr, db):
    bad = make_range_asset(lambda keys: pd.DataFrame({"v": [1]}))
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "has no 'partition_key' column" in _chain_text(ei.value)


def test_range_write_mapping_missing_key_errors(mgr, db):
    bad = make_range_asset(lambda keys: {keys[0]: _day(keys[0])})
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "must equal the materialized partition range" in _chain_text(ei.value)


def test_range_write_non_frame_non_mapping_errors(mgr, db):
    bad = make_range_asset(lambda keys: [1, 2, 3])
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], resources={"io_manager": mgr}, tags=RANGE_TAGS)
    assert "dict {partition_key: DataFrame}" in _chain_text(ei.value)


def test_range_load_returns_one_dataframe_of_only_range_rows(mgr, db):
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
    q = [c["query"] for c in CALLS if "query" in c and '"events"' in c["query"]][-1]
    assert "IN ('2024-01-01', '2024-01-02', '2024-01-03')" in q
    s = _rows(db, "summary")
    assert sorted(s.partition_key) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert set(s.c) == {2}


def test_single_partition_regression(mgr, db):
    for k in ("2024-01-01", "2024-01-02", "2024-01-02"):
        assert dg.materialize([events_single], resources={"io_manager": mgr}, partition_key=k).success
    df = _rows(db)
    assert len(df) == 4
    assert sorted(df.partition_key.unique()) == ["2024-01-01", "2024-01-02"]
    assert [c["rows"] for c in CALLS if "mode" in c] == [2, 2, 2]


def test_single_partition_load_regression(mgr, db):
    seen = {}

    @dg.asset(partitions_def=PARTS)
    def reader(events: pd.DataFrame):
        seen["df"] = events
        return pd.DataFrame({"c": [len(events)]})

    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-01")
    r = dg.materialize([events_single, reader], resources={"io_manager": mgr}, partition_key="2024-01-02")
    assert r.success
    assert set(seen["df"].partition_key) == {"2024-01-02"}


def test_unpartitioned_regression(mgr, db):
    @dg.asset(name="plain")
    def plain():
        return pd.DataFrame({"a": [1, 2, 3]})

    assert dg.materialize([plain], resources={"io_manager": mgr}).success
    assert dg.materialize([plain], resources={"io_manager": mgr}).success
    assert list(_rows(db, "plain").a) == [1, 2, 3]
    assert [c["mode"] for c in CALLS if "mode" in c] == ["overwrite", "overwrite"]
