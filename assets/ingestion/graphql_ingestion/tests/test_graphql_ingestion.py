"""Committed tests for GraphQLIngestionComponent's cosmetic-partitioning
fix + backfill_policy support (see airtable_ingestion's tests for the
full rationale -- same _filter_df_to_partition() fix, applied right
before df.to_sql() since this component writes directly to SQL and
returns no IO-managed value).

Mocks ONLY requests.post (this component imports `requests` lazily inside
the asset body, so the real top-level `requests` module is patched
directly, not `mod.requests`) -- everything else (partitions_def
construction, JSON flattening, destination write, partition filtering) is
exercised for real through dg.materialize() + a real temp-file SQLite
database.

Also regression-tests a real unrelated crash bug found+fixed alongside
the partitioning work: the final metadata dict referenced the undefined
names `include_preview`, `preview_rows`, and bare `MetadataValue` (never
imported -- only `dg.MetadataValue` is used elsewhere in this file) --
this asset crashed on EVERY materialization, unconditionally, regardless
of partitioning.
"""
import requests
import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import FakeGraphQLServer, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, db_url, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_GRAPHQL_ENDPOINT", "https://api.example.com/graphql")
    monkeypatch.setenv("FAKE_DB_URL", db_url)
    kwargs = dict(
        asset_name="gql_data",
        endpoint_env_var="FAKE_GRAPHQL_ENDPOINT",
        database_url_env_var="FAKE_DB_URL",
        queries=[{"name": "items", "query": "{ items { id created_at } }", "data_path": "data.items"}],
    )
    kwargs.update(overrides)
    return mod.GraphQLIngestionComponent(**kwargs)


def _materialize(mod, component, fake_server, monkeypatch, partition_key=None, tags=None):
    monkeypatch.setattr(requests, "post", fake_server.handle_post)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], partition_key=partition_key, tags=tags or {})


def _row_count(db_url, table):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        n = conn.execute(text(f"select count(*) from {table}")).scalar()
    engine.dispose()
    return n


def test_single_partition_filters_to_one_days_rows(mod, tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeGraphQLServer([
        {"id": 1, "created_at": "2024-01-02T10:00:00"},
        {"id": 2, "created_at": "2024-01-02T18:00:00"},
        {"id": 3, "created_at": "2024-01-03T10:00:00"},
    ])
    component = _make_component(
        mod, db_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    result = _materialize(mod, component, fake_server, monkeypatch, partition_key="2024-01-02")
    assert result.success
    assert _row_count(db_url, "gql_data_items") == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, f"sqlite:///{tmp_path / 'test.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, f"sqlite:///{tmp_path / 'test.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_filters_to_the_requested_range_only(mod, tmp_path, monkeypatch):
    """Real bug before the fix: partition_date_column was declared and
    documented but never used -- every partition (and every backfill
    range) silently wrote the IDENTICAL full result set. Confirmed fixed:
    a 2-day single_run backfill (2024-01-02..2024-01-03) now excludes the
    2024-01-04 row that falls outside the requested range."""
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeGraphQLServer([
        {"id": 1, "created_at": "2024-01-02T10:00:00"},
        {"id": 2, "created_at": "2024-01-03T10:00:00"},
        {"id": 3, "created_at": "2024-01-04T10:00:00"},
    ])
    component = _make_component(
        mod, db_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    result = _materialize(
        mod, component, fake_server, monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    assert _row_count(db_url, "gql_data_items") == 2


def test_preview_metadata_does_not_crash(mod, tmp_path, monkeypatch):
    """Real unrelated crash bug found+fixed: include_preview_metadata=True
    referenced undefined names (include_preview, preview_rows, bare
    MetadataValue) in the final metadata dict -- this asset crashed on
    EVERY materialization where any query returned at least one row,
    completely independent of partitioning."""
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeGraphQLServer([{"id": 1, "created_at": "2024-01-02"}])
    component = _make_component(
        mod, db_url, monkeypatch, include_preview_metadata=True,
    )
    result = _materialize(mod, component, fake_server, monkeypatch)
    assert result.success
