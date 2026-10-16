"""Partition-range (single_run backfill) behavior of DatabricksIOManager.

Uses real dg.materialize() runs.  The unavoidable network client (databricks.sql)
is replaced by a fake DB-API module backed by a real sqlite database; the
fake only rewrites dialect-specific statements (three-part names, CREATE SCHEMA, USING DELTA, COPY INTO).  All DELETE /
INSERT / SELECT SQL is built by the real manager code and executed for real,
and every executed statement is captured.
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
EXECUTED = []


def _rewrite(sql):
    if sql.startswith("CREATE SCHEMA"):
        return "SELECT 1"
    sql = re.sub(r"`[^`]+`\.`[^`]+`\.(?=`)", "", sql)
    return sql.replace(" USING DELTA", "")


class _Cursor(sqlite3.Cursor):
    def execute(self, sql, *a):
        EXECUTED.append(sql)
        new = _rewrite(sql)
        m = re.match(r"COPY INTO `(\w+)` FROM '([^']+)'", new)
        if m:  # emulate COPY INTO from the really-written staging parquet
            pd.read_parquet(m.group(2)).to_sql(m.group(1), self.connection, if_exists="append", index=False)
            return super().execute("SELECT 1")
        return super().execute(new, *a)


class _Conn(sqlite3.Connection):
    def cursor(self, factory=None):
        return super().cursor(_Cursor)

    def close(self):
        pass  # keep the in-process database alive across connections


@pytest.fixture(autouse=True)
def fake_client(tmp_path, monkeypatch):
    EXECUTED.clear()
    db = sqlite3.connect(str(tmp_path / "w.db"), factory=_Conn, isolation_level=None, check_same_thread=False)
    pkg = types.ModuleType("databricks")
    sqlmod = types.ModuleType("databricks.sql")
    sqlmod.connect = lambda **kw: db
    pkg.sql = sqlmod
    monkeypatch.setitem(sys.modules, "databricks", pkg)
    monkeypatch.setitem(sys.modules, "databricks.sql", sqlmod)
    return db


@pytest.fixture
def db(fake_client):
    return fake_client


@pytest.fixture
def mgr():
    return mod.DatabricksIOManager(server_hostname="h", http_path="/p", access_token="t")


def _rows(db, table="events"):
    return pd.read_sql(f"SELECT * FROM {table}", db)


def _chain_text(exc):
    out, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        out.append(str(exc))
        exc = exc.__cause__ or exc.__context__
    return " | ".join(out)


def _day(k, n=2, tag=0):
    return pd.DataFrame({"v": [f"{k}-{i}-{tag}" for i in range(n)], "n": list(range(n))})


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
    # the DELETE really scoped the whole range with IN (...), never a bare DELETE
    deletes = [s for s in EXECUTED if s.startswith("DELETE")]
    assert len(deletes) == 1 and "IN ('2024-01-01', '2024-01-02', '2024-01-03')" in deletes[0]
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
    df = _rows(db)
    assert len(df) == 6 and list(df.columns).count("partition_key") == 1


def test_range_write_dataframe_with_foreign_key_errors_and_changes_nothing(mgr, db):
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    bad = make_range_asset(lambda keys: pd.DataFrame({"v": ["x"], "n": [1], "partition_key": ["2030-01-01"]}))
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
    selects = [s for s in EXECUTED if s.startswith("SELECT") and "events" in s]
    assert any("IN ('2024-01-01', '2024-01-02', '2024-01-03')" in s for s in selects)
    s = _rows(db, "summary")
    assert sorted(s.partition_key) == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert set(s.c) == {2}


def test_single_partition_regression(mgr, db):
    for k in ("2024-01-01", "2024-01-02", "2024-01-02"):
        assert dg.materialize([events_single], resources={"io_manager": mgr}, partition_key=k).success
    df = _rows(db)
    assert len(df) == 4
    assert sorted(df.partition_key.unique()) == ["2024-01-01", "2024-01-02"]
    assert any("IN ('2024-01-02')" in s for s in EXECUTED if s.startswith("DELETE"))


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
    assert dg.materialize([plain], resources={"io_manager": mgr}).success  # DELETE + INSERT replaces
    assert list(_rows(db, "plain").a) == [1, 2, 3]


def test_range_write_via_staging_location_copy_into(tmp_path, db):
    stage = tmp_path / "stage"
    stage.mkdir()
    mgr = mod.DatabricksIOManager(
        server_hostname="h", http_path="/p", access_token="t", staging_location=str(stage)
    )
    dg.materialize([events_single], resources={"io_manager": mgr}, partition_key="2024-01-04")
    a = make_range_asset(lambda keys: {k: _day(k) for k in keys})
    for _ in range(2):
        assert dg.materialize([a], resources={"io_manager": mgr}, tags=RANGE_TAGS).success
    df = _rows(db)
    assert len(df) == 8 and len(df[df.partition_key == "2024-01-04"]) == 2
    staged = pd.concat([pd.read_parquet(f) for f in stage.glob("*.parquet")])
    assert {"2024-01-01", "2024-01-02", "2024-01-03"} <= set(staged.partition_key)
    assert any(s.startswith("COPY INTO") for s in EXECUTED)
    assert not any(s == "DELETE FROM `main`.`default`.`events`" for s in EXECUTED)  # never whole-table
