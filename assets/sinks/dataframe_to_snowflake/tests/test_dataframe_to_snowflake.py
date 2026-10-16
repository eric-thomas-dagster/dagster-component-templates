"""Committed tests for DataframeToSnowflakeComponent: resource_key retrofit + backfill_policy.

Only the Snowflake wire boundary is faked: `snowflake.connector.connect` returns a recording
fake connection and `snowflake.connector.pandas_tools.write_pandas` captures the frame. The
resource path uses the REAL dagster_snowflake.SnowflakeResource.get_connection(); the component
runs for real under dg.materialize().
"""
import dagster as dg
import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.pandas_tools
from dagster_snowflake import SnowflakeResource

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


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *a, **k):
        self.conn.executed.append(sql)

    def close(self):
        pass


class FakeConn:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.close_calls = 0
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.close_calls += 1


class Recorder:
    def __init__(self):
        self.conns = []
        self.writes = []  # (conn, df, table_name)
        self.fail_write = False


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def sf(monkeypatch):
    rec = Recorder()

    def fake_connect(**kwargs):
        c = FakeConn(kwargs)
        rec.conns.append(c)
        return c

    def fake_write_pandas(conn, df, table_name, **kwargs):
        if rec.fail_write:
            raise RuntimeError("snowflake exploded")
        rec.writes.append((conn, df.copy(), table_name, kwargs))
        return True, 1, len(df), []

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)
    monkeypatch.setattr(snowflake.connector.pandas_tools, "write_pandas", fake_write_pandas)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
    return rec


def _written_ids(rec):
    assert len(rec.writes) == 1
    return sorted(rec.writes[0][1]["ID"].tolist())


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_sf")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table", "out")
    return mod.DataframeToSnowflakeComponent(**kw)


# ------------------------------------------------------------------ resource_key

def test_resource_key_default_none_inline_env_vars_and_closes_connection(mod, sf):
    sink = _comp(mod, database="DB", schema="SC", warehouse="WH").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_flat_upstream(), sink]).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "env_acct" and conn.kwargs["user"] == "env_user"
    assert conn.kwargs["password"] == "env_pw" and conn.kwargs["warehouse"] == "WH"
    assert conn.close_calls == 1
    assert conn.executed == ["DROP TABLE IF EXISTS DB.SC.OUT"]  # if_exists=replace
    assert sf.writes[0][2] == "OUT"
    assert _written_ids(sf) == [1, 2, 3]


def test_resource_path_uses_real_snowflake_resource_and_ignores_env(mod, sf, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT")  # inline path would raise if it were used
    res = SnowflakeResource(account="res_acct", user="res_user", password="res_pw")
    sink = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"snow"}
    assert dg.materialize([_flat_upstream(), sink], resources={"snow": res}).success
    (conn,) = sf.conns
    assert conn.kwargs["account"] == "res_acct" and conn.kwargs["user"] == "res_user"
    assert conn.close_calls >= 1  # closed by the component (and again by the resource's own exit)
    assert sf.writes[0][0] is conn
    assert _written_ids(sf) == [1, 2, 3]


def test_resource_key_not_registered_fails_loudly(mod, sf):
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_flat_upstream(), sink])


def test_connection_closed_when_write_fails_inline(mod, sf):
    sf.fail_write = True
    sink = _comp(mod).build_defs(None).assets[0]
    with pytest.raises(Exception, match="snowflake exploded"):
        dg.materialize([_flat_upstream(), sink])
    (conn,) = sf.conns
    assert conn.close_calls >= 1


def test_connection_closed_when_write_fails_via_resource(mod, sf):
    sf.fail_write = True
    res = SnowflakeResource(account="a", user="u", password="p")
    sink = _comp(mod, resource_key="snow").build_defs(None).assets[0]
    with pytest.raises(Exception, match="snowflake exploded"):
        dg.materialize([_flat_upstream(), sink], resources={"snow": res})
    (conn,) = sf.conns
    assert conn.close_calls >= 1


# --------------------------------------------------------------- backfill_policy

def _partitioned(mod, **kw):
    return _comp(mod, upstream_asset_key="orders_src", partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, sf):
    assert _partitioned(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, sf):
    up = daily_upstream()
    sink = _partitioned(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _written_ids(sf) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, sf):
    up = daily_upstream()
    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert len(sf.conns) == 1  # one run, one connection
    assert _written_ids(sf) == [101, 102, 201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_sf")[0].metadata["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, sf):
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
    assert _written_ids(sf) == [31, 32, 41, 42]
