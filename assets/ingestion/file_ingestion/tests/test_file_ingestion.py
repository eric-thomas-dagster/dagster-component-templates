"""Committed tests for FileIngestionComponent's backfill_policy='single_run'
support (Branch B / fixed file_path mode only -- see the field's own
docstring for why the sensor-driven and from_upstream branches are not
wired up the same way).

Unlike the "_to_database_asset" family, this asset returns Output(value=df)
(or a bare df) -- it IS routed through Dagster's IO manager, so a
single_run backfill here DOES have the multi-partition-output IO manager
constraint documented on the field (same as the Tier B dlt-based
connectors) -- tests use dg.InMemoryIOManager() + the
`dagster/asset_partition_range_*` run tags a real single_run backfill
sets, same pattern as assets/ingestion/churnzero_ingestion's own tests.

Uses real temp CSV files on disk -- no network/API to mock for this
branch at all.
"""
from pathlib import Path

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, file_path_template, **overrides):
    kwargs = dict(asset_name="daily_sales", file_path=file_path_template)
    kwargs.update(overrides)
    return mod.FileIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_reads_one_file(mod, tmp_path):
    (tmp_path / "sales_2024-01-02.csv").write_text("id,total\n1,10.0\n2,20.0\n")
    component = _make_component(
        mod, str(tmp_path / "sales_{partition_date}.csv"),
        partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path):
    component = _make_component(
        mod, str(tmp_path / "sales_{partition_date}.csv"),
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path):
    component = _make_component(
        mod, str(tmp_path / "sales_{partition_date}.csv"),
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_reads_one_file_per_day_and_concatenates(mod, tmp_path):
    """Real bug before the fix: the asset read exactly ONE file per call,
    templated from a single context.partition_key (parsed as a date),
    which doesn't exist for a multi-partition single_run backfill.
    Confirmed fixed: a 3-day single_run backfill now reads and
    concatenates all 3 days' files."""
    (tmp_path / "sales_2024-01-02.csv").write_text("id,total\n1,10.0\n2,20.0\n")
    (tmp_path / "sales_2024-01-03.csv").write_text("id,total\n3,30.0\n")
    (tmp_path / "sales_2024-01-04.csv").write_text("id,total\n4,40.0\n5,50.0\n6,60.0\n")

    component = _make_component(
        mod, str(tmp_path / "sales_{partition_date}.csv"),
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
    df = result.output_for_node("daily_sales")
    assert len(df) == 6  # 2 + 1 + 3
    assert sorted(df["id"].tolist()) == [1, 2, 3, 4, 5, 6]


def test_single_run_backfill_caches_each_days_file_to_its_own_parquet(mod, tmp_path):
    """cache_to_parquet caches per resolved file path -- confirm a
    single_run backfill writes one cache file per day, not a single
    shared/overwritten one."""
    (tmp_path / "sales_2024-01-02.csv").write_text("id,total\n1,10.0\n")
    (tmp_path / "sales_2024-01-03.csv").write_text("id,total\n2,20.0\n3,30.0\n")

    component = _make_component(
        mod, str(tmp_path / "sales_{partition_date}.csv"),
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run", cache_to_parquet=True,
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    assert (tmp_path / "sales_2024-01-02.csv.parquet").exists()
    assert (tmp_path / "sales_2024-01-03.csv.parquet").exists()
