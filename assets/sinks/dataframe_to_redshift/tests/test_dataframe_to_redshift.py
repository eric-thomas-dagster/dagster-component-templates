"""Committed tests for DataframeToRedshiftComponent: resource_key retrofit + backfill_policy.

psycopg2 / a real Redshift cluster are the external boundary: `sqlalchemy.create_engine` is
monkeypatched to record the URL the component builds and hand back a real SQLite engine, so
pandas.to_sql really writes rows that the tests read back. The component runs for real.
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
def pg(monkeypatch, tmp_path):
    """Patch create_engine: record URLs, return a sqlite engine. Returns (urls, sqlite_url)."""
    sqlite_url = f"sqlite:///{tmp_path}/rs.db"
    real = sqlalchemy.create_engine
    urls = []

    def fake_create_engine(url, *a, **k):
        # Only the component's Redshift URL is intercepted; Dagster's own (sqlite) engines pass through.
        if str(url).startswith("postgresql"):
            urls.append(url)
            return real(sqlite_url)
        return real(url, *a, **k)

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    for k, v in {"REDSHIFT_HOST": "env-host", "REDSHIFT_DATABASE": "env_db",
                 "REDSHIFT_USER": "env_user", "REDSHIFT_PASSWORD": "env_pw"}.items():
        monkeypatch.setenv(k, v)

    class H:
        pass
    h = H()
    h.urls, h.sqlite_url, h.real = urls, sqlite_url, real
    return h


def _read(pg, table="out"):
    eng = pg.real(pg.sqlite_url)
    try:
        return pd.read_sql_table(table, eng)
    finally:
        eng.dispose()


def _ids(pg):
    return sorted(_read(pg)["id"].tolist())


class FakeRedshiftResource:
    """Same attributes as dagster_aws.redshift.RedshiftClientResource."""
    def __init__(self, host, port, user, password, database):
        self.host, self.port, self.user, self.password, self.database = host, port, user, password, database


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_rs")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table", "out")
    kw.setdefault("schema", "main")  # sqlite's only schema; default 'public' would not exist
    return mod.DataframeToRedshiftComponent(**kw)


# ------------------------------------------------------------------ resource_key

def test_resource_key_default_none_inline_env_vars(mod, pg):
    sink = _comp(mod).build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_flat_upstream(), sink]).success
    assert pg.urls == ["postgresql+psycopg2://env_user:env_pw@env-host:5439/env_db"]
    assert _ids(pg) == [1, 2, 3]


def test_resource_path_builds_url_from_resource_and_ignores_env(mod, pg):
    res = FakeRedshiftResource("res-host", 5440, "res_user", "res_pw", "res_db")
    sink = _comp(mod, resource_key="rs_shared").build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"rs_shared"}
    assert dg.materialize([_flat_upstream(), sink], resources={"rs_shared": res}).success
    assert pg.urls == ["postgresql+psycopg2://res_user:res_pw@res-host:5440/res_db"]
    assert _ids(pg) == [1, 2, 3]


def test_resource_path_does_not_need_env_vars(mod, pg, monkeypatch):
    for k in ("REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD"):
        monkeypatch.delenv(k)
    res = FakeRedshiftResource("h", 5439, "u", "p", "d")
    sink = _comp(mod, resource_key="rs_shared").build_defs(None).assets[0]
    assert dg.materialize([_flat_upstream(), sink], resources={"rs_shared": res}).success


def test_inline_path_missing_env_fails_loudly(mod, pg, monkeypatch):
    monkeypatch.delenv("REDSHIFT_HOST")
    sink = _comp(mod).build_defs(None).assets[0]
    with pytest.raises(Exception, match="REDSHIFT_HOST"):
        dg.materialize([_flat_upstream(), sink])


def test_resource_key_not_registered_fails_loudly(mod, pg):
    sink = _comp(mod, resource_key="missing_key").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_flat_upstream(), sink])


def test_password_with_url_special_characters_is_escaped(mod, pg):
    res = FakeRedshiftResource("res-host", 5439, "u", "p@ss/w:rd", "d")
    sink = _comp(mod, resource_key="rs_shared").build_defs(None).assets[0]
    assert dg.materialize([_flat_upstream(), sink], resources={"rs_shared": res}).success
    parsed = sqlalchemy.engine.make_url(pg.urls[0])
    assert (parsed.host, parsed.password, parsed.database) == ("res-host", "p@ss/w:rd", "d")


# --------------------------------------------------------------- backfill_policy

def _partitioned(mod, **kw):
    return _comp(mod, upstream_asset_key="orders_src", partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, pg):
    assert _partitioned(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, pg):
    up = daily_upstream()
    sink = _partitioned(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _ids(pg) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, pg):
    up = daily_upstream()
    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert len(pg.urls) == 1  # one run, one engine
    assert _ids(pg) == [101, 102, 201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_rs")[0].metadata["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, pg):
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
    assert _ids(pg) == [31, 32, 41, 42]
