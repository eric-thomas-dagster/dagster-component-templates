"""Committed tests for DataframeToDatabricksComponent: resource_key retrofit + backfill_policy.

`databricks-sql-connector` is not installed, so a minimal fake `databricks` / `databricks.sql`
(injected with monkeypatch.setitem, undone at test end) is the external boundary: its connection
and cursor record the SQL/rows. The component (and, in one test, the real DatabricksSqlResource)
runs for real under dg.materialize().
"""
import importlib.util
import pathlib
import sys
import types

import dagster as dg
import pandas as pd
import pytest

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

TABLE = "main.sc.out"


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *a):
        self.conn.statements.append(sql)

    def executemany(self, sql, rows):
        self.conn.statements.append(sql)
        self.conn.rows.extend(rows)

    def fetchone(self):
        return (0,)


class FakeConn:
    def __init__(self, kwargs=None):
        self.kwargs = kwargs
        self.statements = []
        self.rows = []
        self.exited = 0

    def cursor(self):
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.exited += 1
        return False


class Recorder:
    def __init__(self):
        self.conns = []


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def dbx(monkeypatch):
    rec = Recorder()
    databricks = types.ModuleType("databricks")
    databricks.__path__ = []
    sql = types.ModuleType("databricks.sql")

    def connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    sql.connect = connect
    databricks.sql = sql
    monkeypatch.setitem(sys.modules, "databricks", databricks)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql)
    monkeypatch.setenv("DATABRICKS_HOST", "env-host.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    return rec


class FakeDatabricksResource:
    """Same API as resources/databricks_sql_resource DatabricksSqlResource.get_connection()."""
    def __init__(self, rec):
        self.rec = rec

    def get_connection(self):
        c = FakeConn({"via": "resource"})
        self.rec.conns.append(c)
        return c


def _written_ids(rec):
    assert len(rec.conns) == 1
    return sorted(r[0] for r in rec.conns[0].rows)


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_dbx")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("schema", "sc")
    kw.setdefault("table", "out")
    return mod.DataframeToDatabricksComponent(**kw)


# ------------------------------------------------------------------ resource_key

def test_resource_key_default_none_inline_env_vars(mod, dbx):
    sink = _comp(mod, cluster_id="wh123").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_flat_upstream(), sink]).success
    (conn,) = dbx.conns
    assert conn.kwargs == {"server_hostname": "env-host.cloud.databricks.com",
                           "http_path": "/sql/1.0/warehouses/wh123", "access_token": "env-token"}
    assert conn.exited == 1
    assert conn.statements[0] == f"CREATE OR REPLACE TABLE {TABLE} (`id` BIGINT, `v` STRING)"
    assert _written_ids(dbx) == [1, 2, 3]


def test_resource_path_runs_same_statements_and_ignores_env(mod, dbx, monkeypatch):
    # Inline run first, to capture the reference statements.
    assert dg.materialize([_flat_upstream(), _comp(mod).build_defs(None).assets[0]]).success
    inline = dbx.conns[0]
    monkeypatch.delenv("DATABRICKS_HOST")  # inline path would raise if it were used
    monkeypatch.delenv("DATABRICKS_TOKEN")
    dbx.conns.clear()
    sink = _comp(mod, resource_key="dbx_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"dbx_shared"}
    assert dg.materialize([_flat_upstream(), sink], resources={"dbx_shared": FakeDatabricksResource(dbx)}).success
    (conn,) = dbx.conns
    assert conn.kwargs == {"via": "resource"}
    assert conn.statements == inline.statements
    assert conn.rows == inline.rows
    assert conn.exited == 1
    assert _written_ids(dbx) == [1, 2, 3]


def test_resource_path_with_real_databricks_sql_resource(mod, dbx):
    here = pathlib.Path(__file__).resolve().parents[4] / "resources" / "databricks_sql_resource" / "component.py"
    spec = importlib.util.spec_from_file_location("databricks_sql_resource_component", here)
    rmod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rmod)
    res = rmod.DatabricksSqlResource(server_hostname="res-host", http_path="/sql/1.0/warehouses/res",
                                     access_token="res-token", catalog="cat")
    sink = _comp(mod, resource_key="dbx_shared").build_defs(None).assets[0]
    assert dg.materialize([_flat_upstream(), sink], resources={"dbx_shared": res}).success
    (conn,) = dbx.conns
    assert conn.kwargs["server_hostname"] == "res-host" and conn.kwargs["catalog"] == "cat"
    assert _written_ids(dbx) == [1, 2, 3]


def test_resource_key_not_registered_fails_loudly(mod, dbx):
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_flat_upstream(), sink])


# --------------------------------------------------------------- backfill_policy

def _partitioned(mod, **kw):
    return _comp(mod, upstream_asset_key="orders_src", partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, dbx):
    assert _partitioned(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, dbx):
    up = daily_upstream()
    sink = _partitioned(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _written_ids(dbx) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, dbx):
    up = daily_upstream()
    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert _written_ids(dbx) == [101, 102, 201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_dbx")[0].metadata["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, dbx):
    """Upstream is NOT partitioned and hands over all 3 days; only the component's own range
    filter can narrow the rows to the 2-day range."""
    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_days, sink], tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _written_ids(dbx) == [31, 32, 41, 42]
