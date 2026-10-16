"""Committed tests for DLTDataFrameWriterComponent: backfill_policy (no resource_key on this sink).

Nothing is mocked: a REAL dlt pipeline writes to a real DuckDB file (destination='duckdb',
duckdb_database_path) and the tests read the table back. This sink loads its upstream with
context.load_asset_value() and no partition key, so the range tests use a partition-agnostic
upstream (all days) -- only the component's own range filter can narrow the rows.
"""
import dagster as dg
import duckdb
import pandas as pd
import pytest

from .conftest import (
    PartitionedMemoryIOManager,
    load_component_module,
)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(tmp_path, monkeypatch):
    # Keep dlt's local pipeline state out of ~/.dlt
    monkeypatch.setenv("PIPELINES_DIR", str(tmp_path / "dlt_pipelines"))
    monkeypatch.chdir(tmp_path)
    return tmp_path / "out.duckdb"


def _ids(db, asset_name="to_dlt", table="orders"):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return sorted(r[0] for r in con.execute(f'SELECT id FROM "{asset_name}"."{table}"').fetchall())
    finally:
        con.close()


def _all_days():
    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)
    return all_days


def _comp(mod, db, **kw):
    return mod.DLTDataFrameWriterComponent(
        asset_name="to_dlt", upstream_asset_keys="orders_src", table="orders",
        destination="duckdb", duckdb_database_path=str(db),
        partition_type="daily", partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, env):
    assert _comp(mod, env).build_defs(None).assets[0].backfill_policy is None
    single = _comp(mod, env, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _comp(mod, env, backfill_policy="multi_run",
                  backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, env):
    sink = _comp(mod, env).build_defs(None).assets[0]
    r = dg.materialize([_all_days(), sink], partition_key="2024-01-03",
                       resources={"io_manager": PartitionedMemoryIOManager()})
    assert r.success
    assert _ids(env) == [31, 32]


def test_single_run_backfill_range_writes_only_keys_in_range(mod, env):
    """Upstream hands over days 2,3,4; the range is 03..04, so only the component's range
    filter can drop day 2 (and a single-key filter would drop day 4's rows too)."""
    sink = _comp(mod, env, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([_all_days(), sink], resources={"io_manager": PartitionedMemoryIOManager()}, tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _ids(env) == [31, 32, 41, 42]


def test_full_range_writes_all_rows_once(mod, env):
    sink = _comp(mod, env, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([_all_days(), sink], resources={"io_manager": PartitionedMemoryIOManager()}, tags={
        "dagster/asset_partition_range_start": "2024-01-02",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _ids(env) == [21, 22, 31, 32, 41, 42]
