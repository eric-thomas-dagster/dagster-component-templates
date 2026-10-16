"""Committed tests for IcebergIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Uses a real local PyIceberg SQL catalog (SQLite-backed metadata, local
filesystem warehouse) with a real table written via table.append() -- no
network/API to mock for this component at all.

This asset returns Output(value=df) -- it IS routed through Dagster's IO
manager, so a single_run backfill here has the multi-partition-output IO
manager constraint documented on the field -- tests use
dg.InMemoryIOManager() + the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets.
"""
import dagster as dg
import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def catalog_properties(tmp_path):
    catalog = load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    catalog.create_namespace("sales")
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "total": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "event_date": [
            "2024-01-02", "2024-01-02",
            "2024-01-03",
            "2024-01-04", "2024-01-04", "2024-01-04",
        ],
    })
    table = catalog.create_table("sales.orders", schema=pa.Table.from_pandas(df).schema)
    table.append(pa.Table.from_pandas(df))
    return {"uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": f"file://{tmp_path}/warehouse"}


def _make_component(mod, catalog_properties, **overrides):
    kwargs = dict(
        asset_name="orders_iceberg",
        catalog_type="sql",
        catalog_name="local",
        catalog_properties=catalog_properties,
        namespace="sales",
        table_name="orders",
        row_filter="event_date = '{partition_key}'",
    )
    kwargs.update(overrides)
    return mod.IcebergIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_scans_one_days_rows(mod, catalog_properties):
    component = _make_component(
        mod, catalog_properties,
        partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("orders_iceberg")
    assert len(df) == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, catalog_properties):
    component = _make_component(
        mod, catalog_properties,
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, catalog_properties):
    component = _make_component(
        mod, catalog_properties,
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_scans_one_query_per_day_and_concatenates(mod, catalog_properties):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into
    row_filter, which doesn't exist for a multi-partition single_run
    backfill -- has_partition_key is False for a range, so partition_key
    silently became None and the unresolved '{partition_key}' literal was
    sent to PyIceberg as the row_filter, which would either error or match
    nothing. Confirmed fixed: a 3-day single_run backfill now issues one
    filtered scan per day in the range and concatenates exactly those
    rows."""
    component = _make_component(
        mod, catalog_properties,
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
    df = result.output_for_node("orders_iceberg")
    assert len(df) == 6
    assert sorted(df["id"].tolist()) == [1, 2, 3, 4, 5, 6]
