"""Committed tests for MongoDBIngestionComponent's cosmetic-partitioning
fix + backfill_policy support (see airtable_ingestion's tests for the
full rationale -- same _filter_df_to_partition() fix, applied to this
component's slightly different shape: an inline dlt source instead of a
dlt verified source import)."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        connection_url="mongodb://localhost:27017",
        database="mydb",
        collection_names=["users"],
    )
    kwargs.update(overrides)
    return mod.MongoDBIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_filters_to_one_days_rows(mod, monkeypatch):
    table = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "created_at": [
            "2024-01-02T10:00:00", "2024-01-02T18:00:00",
            "2024-01-03T10:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    install_fake_dlt(monkeypatch, mod, table_rows={"users": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("test_asset")
    assert sorted(df["id"].tolist()) == [1, 2]


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod)
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_filters_to_the_requested_range_only(mod, monkeypatch):
    """Real bug before the fix: partition_date_column was declared and
    documented but never used -- every partition (and every backfill
    range) silently returned the IDENTICAL full collection. Confirmed
    fixed: a 2-day single_run backfill (2024-01-02..2024-01-03) now
    excludes the 2024-01-04 rows that fall outside the requested range."""
    table = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "created_at": [
            "2024-01-02T10:00:00", "2024-01-02T18:00:00",
            "2024-01-03T10:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    install_fake_dlt(monkeypatch, mod, table_rows={"users": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    result = _materialize(
        component,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    df = result.output_for_node("test_asset")
    assert sorted(df["id"].tolist()) == [1, 2, 3]


def test_partitioned_without_filter_column_configured_is_unfiltered(mod, monkeypatch):
    table = pd.DataFrame({"id": [1, 2, 3], "created_at": ["2024-01-02", "2024-01-03", "2024-01-04"]})
    install_fake_dlt(monkeypatch, mod, table_rows={"users": table})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3
