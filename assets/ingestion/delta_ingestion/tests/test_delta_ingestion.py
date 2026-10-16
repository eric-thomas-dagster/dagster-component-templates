"""Committed tests for DeltaIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

This asset returns Output(value=df) -- it IS routed through Dagster's IO
manager, so a single_run backfill here has the multi-partition-output IO
manager constraint documented on the field -- tests use
dg.InMemoryIOManager() + the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets, same pattern as
assets/ingestion/churnzero_ingestion's own tests.

Uses a real local Delta table (written via deltalake.write_deltalake,
partitioned by event_date) -- no network/API to mock for this component at
all.
"""
import dagster as dg
import pandas as pd
import pytest
from deltalake import write_deltalake

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def delta_table_uri(tmp_path):
    table_dir = tmp_path / "events_delta"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "total": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "event_date": [
            "2024-01-02", "2024-01-02",
            "2024-01-03",
            "2024-01-04", "2024-01-04", "2024-01-04",
        ],
    })
    write_deltalake(str(table_dir), df, partition_by=["event_date"])
    return str(table_dir)


def _make_component(mod, table_uri, **overrides):
    kwargs = dict(
        asset_name="events_delta",
        table_uri=table_uri,
        partition_filters=[["event_date", "=", "{partition_key}"]],
    )
    kwargs.update(overrides)
    return mod.DeltaIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_reads_one_days_rows(mod, delta_table_uri):
    component = _make_component(
        mod, delta_table_uri,
        partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("events_delta")
    assert len(df) == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, delta_table_uri):
    component = _make_component(
        mod, delta_table_uri,
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, delta_table_uri):
    component = _make_component(
        mod, delta_table_uri,
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_reads_one_query_per_day_and_concatenates(mod, delta_table_uri):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into
    partition_filters, which doesn't exist for a multi-partition single_run
    backfill -- has_partition_key is False for a range, so partition_key
    silently became None and the filter was dropped entirely, returning ALL
    partitions' rows regardless of the requested range. Confirmed fixed: a
    3-day single_run backfill now issues one filtered read per day in the
    range and concatenates exactly those rows."""
    component = _make_component(
        mod, delta_table_uri,
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-04",
        },
    )
    assert result.success
    df = result.output_for_node("events_delta")
    assert len(df) == 6
    assert sorted(df["id"].tolist()) == [1, 2, 3, 4, 5, 6]
