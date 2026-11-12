"""Committed tests for SapHanaIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout, plus the
resource_key retrofit (resources/sap_hana_resource).

`connection_url_env_var` is read as a literal SQLAlchemy URL with no HANA-
specific parsing in between -- pointing it at a real local SQLite database
exercises the actual sqlalchemy.create_engine()/pd.read_sql() code path for
real, no network/API or HANA driver to mock at all. The resource_key path is
exercised against a FakeHanaResource mirroring resources/sap_hana_resource's
get_engine() shape -- also a real SQLite engine under the hood, so the
component runs for real either way.
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
def sales_db(tmp_path):
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "total": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "business_date": [
            "2024-01-02", "2024-01-02",
            "2024-01-03",
            "2024-01-04", "2024-01-04", "2024-01-04",
        ],
    })
    df.to_sql("sales", engine, index=False)
    engine.dispose()
    return f"sqlite:///{db_path}"


def _make_component(mod, connection_url, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_HANA_URL", connection_url)
    kwargs = dict(
        asset_name="daily_sales",
        connection_url_env_var="FAKE_HANA_URL",
        query="SELECT id, total FROM sales WHERE business_date = '{partition_key}'",
    )
    kwargs.update(overrides)
    return mod.SapHanaIngestionComponent(**kwargs)


def _materialize(component, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_reads_one_days_rows(mod, sales_db, monkeypatch):
    component = _make_component(
        mod, sales_db, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(component, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, sales_db, monkeypatch):
    component = _make_component(
        mod, sales_db, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, sales_db, monkeypatch):
    component = _make_component(
        mod, sales_db, monkeypatch,
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_runs_one_query_per_day_and_concatenates(mod, sales_db, monkeypatch):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into the
    query, which doesn't exist for a multi-partition single_run backfill --
    has_partition_key is False for a range, so partition_key silently
    became None and the unresolved '{partition_key}' literal was sent as
    literal SQL, matching nothing. Confirmed fixed: a 3-day single_run
    backfill now issues one query per day in the range and concatenates
    exactly those rows."""
    component = _make_component(
        mod, sales_db, monkeypatch,
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
    assert len(df) == 6
    assert sorted(df["id"].tolist()) == [1, 2, 3, 4, 5, 6]


# ------------------------------------------------------------------ resource_key

class FakeHanaResource:
    """Same get_engine() shape as resources/sap_hana_resource.HanaResource."""

    def __init__(self, engine):
        self._engine = engine

    def get_engine(self):
        return self._engine


def test_resource_key_default_none_uses_inline_connection(mod, sales_db, monkeypatch):
    component = _make_component(mod, sales_db, monkeypatch, query="SELECT id, total FROM sales")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == set()
    result = _materialize(component)
    assert result.success
    assert len(result.output_for_node("daily_sales")) == 6


def test_resource_key_set_uses_registered_resource_engine(mod, sales_db, monkeypatch):
    # Deliberately NOT setting FAKE_HANA_URL / host / password_env_var: the resource path
    # must not touch any of the inline connection fields.
    engine = create_engine(sales_db)
    res = FakeHanaResource(engine)
    component = mod.SapHanaIngestionComponent(
        asset_name="daily_sales",
        resource_key="hana_shared",
        query="SELECT id, total FROM sales WHERE business_date = '{partition_key}'",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.required_resource_keys - {"io_manager"} == {"hana_shared"}
    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager(), "hana_shared": res},
        partition_key="2024-01-04",
    )
    assert result.success
    df = result.output_for_node("daily_sales")
    assert sorted(df["id"].tolist()) == [4, 5, 6]


def test_resource_key_not_registered_fails_loudly(mod):
    component = mod.SapHanaIngestionComponent(
        asset_name="daily_sales", resource_key="missing_key", query="SELECT 1"
    )
    asset_def = component.build_defs(context=None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
