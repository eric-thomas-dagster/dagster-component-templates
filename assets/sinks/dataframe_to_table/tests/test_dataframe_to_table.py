"""Committed tests for DataframeToTableComponent: resource_key retrofit + backfill_policy.

Real SQLite via SQLAlchemy is the backend (nothing mocked); the component runs for real
under dg.materialize(). Range tests use the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets plus PartitionedMemoryIOManager.
"""
import dagster as dg
import pandas as pd
import pytest
import sqlalchemy

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


@pytest.fixture()
def db(tmp_path):
    path = tmp_path / "t.db"
    return path, f"sqlite:///{path}"


def _read(url, table="out"):
    eng = sqlalchemy.create_engine(url)
    try:
        return pd.read_sql_table(table, eng)
    finally:
        eng.dispose()


def _ids(url, table="out"):
    return sorted(_read(url, table)["id"].tolist())


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


class EngineResource:
    """Exposes get_engine() like the SQL resource components."""
    def __init__(self, url):
        self.url = url
        self.calls = 0

    def get_engine(self):
        self.calls += 1
        return sqlalchemy.create_engine(self.url)


class ConnStringResource:
    """Exposes only a SQLAlchemy-URL connection_string (like resources/postgres_resource)."""
    def __init__(self, url):
        self.connection_string = url


class NeitherResource:
    pass


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_table")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table", "out")
    return mod.DataframeToTableComponent(**kw)


# ------------------------------------------------------------------ resource_key

def test_resource_key_default_none_inline_database_url(mod, db):
    path, url = db
    sink = _comp(mod, database_url=url).build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_flat_upstream(), sink]).success
    assert _ids(url) == [1, 2, 3]


def test_resource_get_engine_path_ignores_inline_url(mod, db, tmp_path, monkeypatch):
    path, url = db
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path}/WRONG.db")
    res = EngineResource(url)
    sink = _comp(mod, resource_key="sql_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"sql_shared"}
    assert dg.materialize([_flat_upstream(), sink], resources={"sql_shared": res}).success
    assert res.calls == 1
    assert _ids(url) == [1, 2, 3]
    assert not (tmp_path / "WRONG.db").exists()


def test_resource_connection_string_branch(mod, db):
    path, url = db
    sink = _comp(mod, resource_key="pg").build_defs(None).assets[0]
    assert dg.materialize([_flat_upstream(), sink], resources={"pg": ConnStringResource(url)}).success
    assert _ids(url) == [1, 2, 3]


def test_resource_with_neither_api_raises_clear_error(mod, db):
    sink = _comp(mod, resource_key="bad").build_defs(None).assets[0]
    with pytest.raises(Exception, match="neither .get_engine"):
        dg.materialize([_flat_upstream(), sink], resources={"bad": NeitherResource()})


def test_resource_key_not_registered_fails_loudly(mod, db):
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_flat_upstream(), sink])


# --------------------------------------------------------------- backfill_policy

def _partitioned(mod, url, **kw):
    return _comp(mod, upstream_asset_key="orders_src", database_url=url, partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, db):
    _, url = db
    assert _partitioned(mod, url).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, url, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, url, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, db):
    _, url = db
    up = daily_upstream()
    sink = _partitioned(mod, url).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _ids(url) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, db):
    _, url = db
    up = daily_upstream()
    sink = _partitioned(mod, url, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert _ids(url) == [101, 102, 201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_table")[0].metadata["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, db):
    """Upstream is NOT partitioned and hands over all 3 days; only the component's own range
    filter can narrow the rows to the 2-day range."""
    _, url = db

    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned(mod, url, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_days, sink], tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _ids(url) == [31, 32, 41, 42]
