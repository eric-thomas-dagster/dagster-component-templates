"""Committed tests for DremioIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Mocks ONLY the Dremio REST API (requests.post for job submit, requests.get
for job-status poll + paged results) via a fake server keyed by the exact
submitted SQL string -- every other behavior (partitions_def construction,
query templating, DataFrame building, preview metadata) is exercised for
real through dg.materialize().

This asset returns Output(value=df) -- it IS routed through Dagster's IO
manager, so a single_run backfill here has the multi-partition-output IO
manager constraint documented on the field -- tests use
dg.InMemoryIOManager() + the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets.
"""
from unittest.mock import MagicMock

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeDremioServer:
    """Maps an exact SQL string to (columns, rows); records every submitted
    query and returns one COMPLETED job per submission, in one page."""

    def __init__(self, sql_to_result: dict):
        self.sql_to_result = sql_to_result
        self.submitted_sqls: list = []

    def handle_post(self, url, json=None, **kwargs):
        assert url.endswith("/api/v3/sql")
        sql = json["sql"]
        self.submitted_sqls.append(sql)
        job_id = str(len(self.submitted_sqls) - 1)
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        resp.json.return_value = {"id": job_id}
        return resp

    def handle_get(self, url, params=None, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        if url.endswith("/results"):
            job_id = url.split("/job/")[1].split("/results")[0]
            sql = self.submitted_sqls[int(job_id)]
            columns, rows = self.sql_to_result.get(sql, ([], []))
            resp.json.return_value = {
                "schema": [{"name": c} for c in columns],
                "rows": rows,
                "rowCount": len(rows),
            }
        else:
            resp.json.return_value = {"jobState": "COMPLETED"}
        return resp


def _make_component(mod, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_DREMIO_PAT", "test-token")
    kwargs = dict(
        asset_name="daily_sales",
        host="http://dremio.example.com",
        auth_type="pat",
        auth_token_env_var="FAKE_DREMIO_PAT",
        query=(
            "SELECT id, total FROM sales "
            "WHERE business_date = DATE '{partition_key}'"
        ),
    )
    kwargs.update(overrides)
    return mod.DremioIngestionComponent(**kwargs)


def _materialize(mod, component, fake, monkeypatch, partition_key=None, tags=None):
    monkeypatch.setattr(mod.requests, "post", fake.handle_post)
    monkeypatch.setattr(mod.requests, "get", fake.handle_get)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_runs_one_query(mod, monkeypatch):
    sql = "SELECT id, total FROM sales WHERE business_date = DATE '2024-01-02'"
    fake = FakeDremioServer({sql: (["id", "total"], [[1, 10.0], [2, 20.0]])})
    component = _make_component(
        mod, monkeypatch, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(mod, component, fake, monkeypatch, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 2
    assert len(fake.submitted_sqls) == 1


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, monkeypatch):
    component = _make_component(
        mod, monkeypatch, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, monkeypatch):
    component = _make_component(
        mod, monkeypatch, partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_runs_one_query_per_day_and_concatenates(mod, monkeypatch):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into the
    query, which doesn't exist for a multi-partition single_run backfill --
    has_partition_key is False for a range, so partition_key silently
    became None and {partition_key} was never substituted, submitting the
    raw unresolved query template as literal SQL. Confirmed fixed: a 3-day
    single_run backfill now submits one resolved query per day in the range
    and concatenates exactly those rows."""
    fake = FakeDremioServer({
        "SELECT id, total FROM sales WHERE business_date = DATE '2024-01-02'": (
            ["id", "total"], [[1, 10.0], [2, 20.0]],
        ),
        "SELECT id, total FROM sales WHERE business_date = DATE '2024-01-03'": (
            ["id", "total"], [[3, 30.0]],
        ),
        "SELECT id, total FROM sales WHERE business_date = DATE '2024-01-04'": (
            ["id", "total"], [[4, 40.0], [5, 50.0], [6, 60.0]],
        ),
    })
    component = _make_component(
        mod, monkeypatch, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    result = _materialize(
        mod, component, fake, monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-04",
        },
    )
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 6
    assert sorted(df["id"].tolist()) == [1, 2, 3, 4, 5, 6]
    assert len(fake.submitted_sqls) == 3
