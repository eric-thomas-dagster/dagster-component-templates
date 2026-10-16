"""Committed tests for DuckDBTableWriterComponent: resource_key retrofit + backfill_policy.

Real duckdb files throughout (inline database_path and a real dagster_duckdb.DuckDBResource);
nothing mocked. The sink loads its upstream via context.load_asset_value (deps wiring), so the
upstream's IO manager is PartitionedMemoryIOManager.
"""
import dagster as dg
import duckdb
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from .conftest import (
    PartitionedMemoryIOManager,
    daily_upstream,
    load_component_module,
    materialize_upstream_days,
)

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-02",
    "dagster/asset_partition_range_end": "2024-01-04",
}


@pytest.fixture()
def mod():
    return load_component_module()


def _ids(path, table="out"):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return sorted(r[0] for r in con.execute(f"SELECT id FROM {table}").fetchall())
    finally:
        con.close()


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_duck")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table", "out")
    return mod.DuckDBTableWriterComponent(**kw)


def _mat_flat(sink, resources=None):
    io = PartitionedMemoryIOManager()
    res = {"io_manager": io}
    res.update(resources or {})
    return dg.materialize([_flat_upstream(), sink], resources=res)


# ------------------------------------------------------------------ resource_key

def test_resource_key_default_none_inline_database_path(mod, tmp_path):
    db = tmp_path / "inline.duckdb"
    sink = _comp(mod, database_path=str(db)).build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert _mat_flat(sink).success
    assert _ids(db) == [1, 2, 3]


def test_resource_path_uses_duckdb_resource_and_ignores_inline_path(mod, tmp_path):
    shared, inline = tmp_path / "shared.duckdb", tmp_path / "inline_ignored.duckdb"
    sink = _comp(mod, database_path=str(inline), resource_key="duck").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"duck"}
    assert _mat_flat(sink, {"duck": DuckDBResource(database=str(shared))}).success
    assert _ids(shared) == [1, 2, 3]
    assert not inline.exists()


def test_resource_key_not_registered_fails_loudly(mod, tmp_path):
    sink = _comp(mod, database_path=str(tmp_path / "x.duckdb"), resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        _mat_flat(sink)


def test_append_mode_accumulates_via_resource(mod, tmp_path):
    shared = tmp_path / "shared.duckdb"
    sink = _comp(mod, resource_key="duck", write_mode="append").build_defs(None).assets[0]
    for _ in range(2):
        assert _mat_flat(sink, {"duck": DuckDBResource(database=str(shared))}).success
    assert _ids(shared) == [1, 1, 2, 2, 3, 3]


# --------------------------------------------------------------- backfill_policy

def _partitioned(mod, db, **kw):
    return _comp(mod, upstream_asset_key="orders_src", database_path=str(db), partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, tmp_path):
    db = tmp_path / "d.duckdb"
    assert _partitioned(mod, db).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, db, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, db, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, tmp_path):
    db = tmp_path / "d.duckdb"
    up = daily_upstream()
    sink = _partitioned(mod, db).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _ids(db) == [201, 202]


def test_single_run_backfill_range_loads_each_partition_and_writes_all_once(mod, tmp_path):
    """The range branch calls load_asset_value(partition_key=k) per key and concatenates. A recording
    IO manager proves each in-range partition was individually loaded ."""
    db = tmp_path / "d.duckdb"
    up = daily_upstream()
    sink = _partitioned(mod, db, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    loaded = []
    orig = io.load_input

    def recording(context):
        loaded.append(tuple(context.asset_partition_keys) if context.has_asset_partitions else None)
        return orig(context)

    io.load_input = recording
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert _ids(db) == [101, 102, 201, 202, 301, 302]
    assert sorted(loaded) == [("2024-01-02",), ("2024-01-03",), ("2024-01-04",)]


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, tmp_path):
    """Unpartitioned upstream returns all 3 days regardless of partition_key; only the component's
    own range filter can narrow to the 2-day range."""
    db = tmp_path / "d.duckdb"

    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned(mod, db, backfill_policy="single_run").build_defs(None).assets[0]
    io = PartitionedMemoryIOManager()
    r = dg.materialize([all_days, sink], resources={"io_manager": io}, tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _ids(db) == [31, 32, 41, 42]
