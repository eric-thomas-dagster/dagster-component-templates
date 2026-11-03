"""Multi-table and partitioned DatabaseReplicationComponent, end to end against two DuckDB files.

Needs `dagster-sling` and the Sling binary. Run: pytest tests/test_database_replication_streams.py
"""
import importlib.util
from pathlib import Path

import dagster as dg
import duckdb
import pytest

COMPONENT = Path(__file__).resolve().parents[1] / "component.py"


@pytest.fixture()
def repl():
    spec = importlib.util.spec_from_file_location("database_replication_component", COMPONENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def dbs(tmp_path):
    src, tgt = tmp_path / "src.duckdb", tmp_path / "tgt.duckdb"
    c = duckdb.connect(str(src))
    c.execute("create schema if not exists main")
    c.execute("create table main.events as select range+1 as id, (date '2024-01-01' + (range//3)::int) as created_at, range*10 as amount from range(15)")
    c.execute("create table main.refunds as select range+1 as id, (date '2024-01-01' + (range//2)::int) as created_at, range as amount from range(10)")
    c.close()
    return src, tgt


def _component(repl, dbs, **kw):
    src, tgt = dbs
    return repl.DatabaseReplicationComponent(
        asset_name="shop", source_type="duckdb", target_type="duckdb",
        source_connection=f"duckdb://{src}", target_connection=f"duckdb://{tgt}", **kw,
    ).build_defs(None)


def _by_day(tgt, table):
    t = duckdb.connect(str(tgt), read_only=True)
    try:
        return {str(d): (n, s) for d, n, s in t.execute(
            f"select cast(created_at as date), count(*), sum(amount) from main.{table} group by 1").fetchall()}
    finally:
        t.close()


def _run(defs, partition_key=None):
    return dg.materialize(list(defs.assets), resources=defs.resources, partition_key=partition_key, raise_on_error=False).success


STREAMS = [
    {"source_table": "main.events", "target_table": "main.events_copy"},
    {"source_table": "main.refunds", "target_table": "main.refunds_copy"},
]


def test_one_asset_per_table(repl, dbs):
    defs = _component(repl, dbs, streams=STREAMS)
    assert sorted(k.to_user_string() for a in defs.assets for k in a.keys) == ["shop/main/events_copy", "shop/main/refunds_copy"]


def test_single_table_keeps_its_asset_name(repl, dbs):
    defs = _component(repl, dbs, source_table="main.refunds", target_table="main.refunds_one")
    assert [k.to_user_string() for a in defs.assets for k in a.keys] == ["shop"]
    assert _run(defs)
    assert len(_by_day(dbs[1], "refunds_one")) == 5


def test_missing_connection_variable_does_not_fail_loading(repl):
    comp = repl.DatabaseReplicationComponent(
        asset_name="x", source_type="postgres", target_type="snowflake",
        source_connection_env_var="NOPE_SRC", target_connection_env_var="NOPE_TGT", source_table="a", target_table="b")
    assert comp.build_defs(None).assets


def test_partition_replaces_only_its_window_and_can_be_re_replicated(repl, dbs):
    src, tgt = dbs
    defs = _component(repl, dbs, streams=STREAMS, partition_type="daily", partition_start="2024-01-01",
                      partition_column="created_at", primary_key=["id"])
    assert _run(defs, "2024-01-02")
    assert _by_day(tgt, "events_copy") == {"2024-01-02": (3, 120)}
    assert _run(defs, "2024-01-03")
    assert set(_by_day(tgt, "events_copy")) == {"2024-01-02", "2024-01-03"}

    c = duckdb.connect(str(src))
    c.execute("update main.events set amount = amount + 1000 where created_at = date '2024-01-02'")
    c.execute("delete from main.events where id = 8")
    c.close()

    assert _run(defs, "2024-01-03")  # only this slice is redone, and the deleted row goes away
    assert _by_day(tgt, "events_copy")["2024-01-03"] == (2, 140)
    assert _by_day(tgt, "events_copy")["2024-01-02"] == (3, 120)  # untouched
    assert _run(defs, "2024-01-02")
    assert _by_day(tgt, "events_copy")["2024-01-02"] == (3, 3120)


def test_partitioned_needs_a_primary_key_and_a_column(repl, dbs):
    with pytest.raises(ValueError, match="primary_key"):
        _component(repl, dbs, streams=STREAMS, partition_type="daily", partition_start="2024-01-01", partition_column="created_at")
    with pytest.raises(ValueError, match="partition_column"):
        _component(repl, dbs, streams=STREAMS, partition_type="daily", partition_start="2024-01-01", primary_key=["id"])


def test_incremental_streams_only_load_new_rows(repl, dbs):
    src, tgt = dbs
    streams = [{**s, "incremental_column": "created_at", "primary_key": ["id"]} for s in STREAMS]
    defs = _component(repl, dbs, streams=streams, mode="incremental")
    assert _run(defs)
    c = duckdb.connect(str(src)); c.execute("insert into main.events values (100, date '2024-01-06', 7)"); c.close()
    assert _run(defs)
    assert _by_day(tgt, "events_copy")["2024-01-06"] == (1, 7)
