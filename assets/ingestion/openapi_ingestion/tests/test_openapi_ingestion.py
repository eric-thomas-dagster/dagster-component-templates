"""Committed tests for OpenAPIIngestionComponent's cosmetic-partitioning
fix + backfill_policy support -- AND a regression test for a real,
unrelated, severe crash bug found+fixed alongside it: `_build_openapi_defs`
(a plain module-level function, not a method) referenced `self.partition_type`
/ `self.retry_policy_max_retries` etc. with NO `self` defined anywhere in
scope, so this component crashed with NameError on EVERY materialization,
unconditionally, regardless of partitioning. Confirmed via direct
reproduction before the fix.

Mocks ONLY requests.get (this component imports `requests` lazily inside
the asset body, so the real top-level `requests` module is patched
directly) -- everything else (endpoint grouping, pagination, real SQL
write, partition filtering) is exercised for real through
dg.materialize() + a real temp-file SQLite database.
"""
import requests
import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import FakeOpenAPIServer, load_component_module, make_grouped


@pytest.fixture()
def mod():
    return load_component_module()


def _build_defs(mod, db_url, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("FAKE_DB_URL", db_url)
    kwargs = dict(
        grouped=make_grouped(),
        api_base_url_env_var="FAKE_API_BASE_URL",
        api_key_env_var=None,
        auth_header="Authorization",
        auth_prefix="Bearer",
        database_url_env_var="FAKE_DB_URL",
        schema_name=None,
        if_exists="replace",
        group_name="openapi",
        page_size=100,
        max_pages=100,
    )
    kwargs.update(overrides)
    return mod._build_openapi_defs(**kwargs)


def _materialize(mod, defs, fake_server, monkeypatch, partition_key=None, tags=None):
    monkeypatch.setattr(requests, "get", fake_server.handle_get)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], partition_key=partition_key, tags=tags or {})


def _row_count(db_url, table):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        n = conn.execute(text(f"select count(*) from {table}")).scalar()
    engine.dispose()
    return n


def test_unpartitioned_materialize_does_not_crash(mod, tmp_path, monkeypatch):
    """Direct regression test for the self-NameError crash: even the
    simplest unpartitioned case went through `_build_openapi_defs`'s
    broken `if self.partition_type:` check and crashed every time."""
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeOpenAPIServer([{"id": 1, "created_at": "2024-01-02"}])
    defs = _build_defs(mod, db_url, monkeypatch)
    result = _materialize(mod, defs, fake_server, monkeypatch)
    assert result.success
    assert _row_count(db_url, "pets") == 1


def test_single_partition_filters_to_one_days_rows(mod, tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeOpenAPIServer([
        {"id": 1, "created_at": "2024-01-02T10:00:00"},
        {"id": 2, "created_at": "2024-01-02T18:00:00"},
        {"id": 3, "created_at": "2024-01-03T10:00:00"},
    ])
    defs = _build_defs(
        mod, db_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    result = _materialize(mod, defs, fake_server, monkeypatch, partition_key="2024-01-02")
    assert result.success
    assert _row_count(db_url, "pets") == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path, monkeypatch):
    defs = _build_defs(
        mod, f"sqlite:///{tmp_path / 'test.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    asset_def = list(defs.assets)[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path, monkeypatch):
    defs = _build_defs(
        mod, f"sqlite:///{tmp_path / 'test.db'}", monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at",
    )
    asset_def = list(defs.assets)[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_filters_to_the_requested_range_only(mod, tmp_path, monkeypatch):
    """Real bug before the fix: partition_date_column was declared but
    never used (and the whole function couldn't even run due to the
    self-NameError crash). Confirmed fixed: a 2-day single_run backfill
    (2024-01-02..2024-01-03) now excludes the 2024-01-04 row that falls
    outside the requested range."""
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    fake_server = FakeOpenAPIServer([
        {"id": 1, "created_at": "2024-01-02T10:00:00"},
        {"id": 2, "created_at": "2024-01-03T10:00:00"},
        {"id": 3, "created_at": "2024-01-04T10:00:00"},
    ])
    defs = _build_defs(
        mod, db_url, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        partition_date_column="created_at", backfill_policy="single_run",
    )
    result = _materialize(
        mod, defs, fake_server, monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    assert _row_count(db_url, "pets") == 2
