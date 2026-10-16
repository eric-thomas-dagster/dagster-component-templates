"""Committed tests for DatabaseQueryComponent's backfill_policy support.

Uses a real SQLite database (no network/paid call to mock). The asset returns
a DataFrame routed through the IO manager, so dg.InMemoryIOManager() plus the
`dagster/asset_partition_range_*` run tags a real single_run backfill sets.
"""
import dagster as dg
import pandas as pd
import pytest
from sqlalchemy import create_engine

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_url(tmp_path):
    url = f"sqlite:///{tmp_path / 'events.db'}"
    pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "event_date": ["2024-01-02", "2024-01-02", "2024-01-03",
                       "2024-01-04", "2024-01-04", "2024-01-04"],
    }).to_sql("events", create_engine(url), index=False)
    return url


QUERY = "SELECT id, event_date FROM events WHERE event_date = date(:partition_date)"


def _component(mod, db_url, **kw):
    return mod.DatabaseQueryComponent(
        asset_name="events_q", database_url=db_url, query=QUERY,
        partition_type="daily", partition_start="2024-01-01", **kw,
    )


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key, tags=tags or {},
    )


def test_single_partition_runs_one_days_query(mod, db_url):
    r = _materialize(_component(mod, db_url), partition_key="2024-01-02")
    assert r.success
    assert sorted(r.output_for_node("events_q")["id"]) == [1, 2]


def test_backfill_policy_wired_and_default_none(mod, db_url):
    a = _component(mod, db_url, backfill_policy="single_run").build_defs(context=None).assets[0]
    assert a.backfill_policy == dg.BackfillPolicy.single_run()
    m = _component(mod, db_url, backfill_policy="multi_run",
                   backfill_policy_max_partitions_per_run=3).build_defs(context=None).assets[0]
    assert m.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    assert _component(mod, db_url).build_defs(context=None).assets[0].backfill_policy is None


def test_single_run_backfill_runs_one_query_per_day(mod, db_url):
    """Before the fix: has_partition_key is False for a range, so no
    :partition_date was bound and the query failed (missing bind parameter)
    instead of running once per day."""
    r = _materialize(
        _component(mod, db_url, backfill_policy="single_run"),
        tags={"dagster/asset_partition_range_start": "2024-01-02",
              "dagster/asset_partition_range_end": "2024-01-04"},
    )
    assert r.success
    df = r.output_for_node("events_q")
    assert sorted(df["id"]) == [1, 2, 3, 4, 5, 6]
    assert len(df) == 6


def test_single_run_backfill_partial_range_only_returns_requested_days(mod, db_url):
    r = _materialize(
        _component(mod, db_url, backfill_policy="single_run"),
        tags={"dagster/asset_partition_range_start": "2024-01-03",
              "dagster/asset_partition_range_end": "2024-01-04"},
    )
    assert sorted(r.output_for_node("events_q")["id"]) == [3, 4, 5, 6]
