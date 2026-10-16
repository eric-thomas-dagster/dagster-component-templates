"""Committed tests for AirtableIngestionComponent's cosmetic-partitioning
fix + backfill_policy support.

Real pre-existing bug (shared by 23 other ingestion components): partition_type
could be configured and partition_date_column/partition_static_column were
declared and documented ("Column used to filter the upstream DataFrame to
the current date/static partition value"), but nothing in the asset body
ever actually filtered anything -- every partition silently returned the
identical full dataset. Fixed via _filter_df_to_partition(), applied right
before the final Output(value=...).

Fakes the dlt pipeline run + sql_client() query-back wholesale (this
repo's "mock only the paid call" convention, see conftest.py) -- table-name
normalization, DataFrame concatenation, partition filtering, and preview
metadata are all exercised for real through dg.materialize().
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="customers",
        api_key="fake-key",
        base_id="appFAKE123",
        table_names=["Customers"],
    )
    kwargs.update(overrides)
    return mod.AirtableIngestionComponent(**kwargs)


def _materialize(component, fake_pipeline, partition_key=None, tags=None):
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
    fake_pipeline, _ = install_fake_dlt(monkeypatch, mod, table_rows={"customers": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    result = _materialize(component, fake_pipeline, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("customers")
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
    range) silently returned the IDENTICAL full table. Confirmed fixed: a
    2-day single_run backfill (2024-01-02..2024-01-03) now excludes the
    2024-01-04 rows that fall outside the requested range."""
    table = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "created_at": [
            "2024-01-02T10:00:00", "2024-01-02T18:00:00",
            "2024-01-03T10:00:00",
            "2024-01-04T06:00:00", "2024-01-04T12:00:00", "2024-01-04T18:00:00",
        ],
    })
    fake_pipeline, _ = install_fake_dlt(monkeypatch, mod, table_rows={"customers": table})
    component = _make_component(
        mod,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    result = _materialize(
        component, fake_pipeline,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    df = result.output_for_node("customers")
    assert sorted(df["id"].tolist()) == [1, 2, 3]


def test_static_partition_filters_by_column(mod, monkeypatch):
    table = pd.DataFrame({
        "id": [1, 2, 3],
        "store_id": ["store_a", "store_a", "store_b"],
    })
    fake_pipeline, _ = install_fake_dlt(monkeypatch, mod, table_rows={"customers": table})
    component = _make_component(
        mod,
        partition_type="static", partition_values="store_a,store_b",
        partition_static_column="store_id",
    )
    result = _materialize(component, fake_pipeline, partition_key="store_a")
    assert result.success
    df = result.output_for_node("customers")
    assert sorted(df["id"].tolist()) == [1, 2]


def test_partitioned_without_filter_column_configured_is_unfiltered(mod, monkeypatch):
    """Backward-compat: partition_date_column/partition_static_column are
    optional. Without one configured, a partitioned asset still returns
    the full table (pre-existing documented behavior for users who only
    wanted scheduling cadence, not filtering)."""
    table = pd.DataFrame({"id": [1, 2, 3], "created_at": ["2024-01-02", "2024-01-03", "2024-01-04"]})
    fake_pipeline, _ = install_fake_dlt(monkeypatch, mod, table_rows={"customers": table})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, fake_pipeline, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("customers")
    assert len(df) == 3
