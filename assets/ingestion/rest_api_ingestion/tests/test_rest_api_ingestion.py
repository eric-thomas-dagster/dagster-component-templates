"""Committed tests for RestApiIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

This is the most structurally complex Tier C component (endpoints: dispatch,
sinks with upsert_on_match, multiple output_format modes) -- backfill_policy
is scoped narrowly (output_format='dataframe', partition_type != 'multi',
enforced by build_defs() raising a clear ValueError otherwise) rather than
generalized across every mode, since concatenating a range of json/csv/
parquet/text responses has no single well-defined behavior.

Mocks ONLY requests.request (keyed by the resolved URL's trailing date) --
every other behavior (partitions_def construction, {partition_key}
templating, DataFrame building, sinks) is exercised for real through
dg.materialize(), including a real local DuckDB sink connection.
"""
from unittest.mock import MagicMock

import dagster as dg
import duckdb
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeRestServer:
    """Maps the URL's trailing `/<date>` segment to canned records; records
    every requested URL."""

    def __init__(self, date_to_records: dict):
        self.date_to_records = date_to_records
        self.urls: list = []

    def handle_request(self, method, url, **kwargs):
        self.urls.append(url)
        date = url.rsplit("/", 1)[-1]
        records = self.date_to_records.get(date, [])
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        resp.status_code = 200
        resp.json.return_value = records
        return resp


class FakeDuckDBResource:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        return self._conn


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="daily_sales",
        api_url="https://api.example.com/sales/{partition_key}",
        output_format="dataframe",
    )
    kwargs.update(overrides)
    return mod.RestApiIngestionComponent(**kwargs)


def _materialize(mod, component, fake, monkeypatch, resources=None, partition_key=None, tags=None):
    monkeypatch.setattr(mod.requests, "request", fake.handle_request)
    asset_def = component.build_defs(context=None).assets[0]
    all_resources = {"io_manager": dg.InMemoryIOManager()}
    all_resources.update(resources or {})
    return dg.materialize(
        [asset_def],
        resources=all_resources,
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_fetches_one_days_records(mod, monkeypatch):
    fake = FakeRestServer({"2024-01-02": [{"id": 1}, {"id": 2}]})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(mod, component, fake, monkeypatch, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 2
    assert len(fake.urls) == 1


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod):
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod):
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_backfill_policy_requires_dataframe_output_format(mod):
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        output_format="json", backfill_policy="single_run",
    )
    with pytest.raises(ValueError, match="output_format='dataframe'"):
        component.build_defs(context=None)


def test_backfill_policy_rejects_multi_partition_type(mod):
    component = _make_component(
        mod, partition_type="multi", partition_start="2024-01-01",
        partition_values=["a", "b"], partition_static_dim="segment",
        backfill_policy="single_run",
    )
    with pytest.raises(ValueError, match="partition_type='multi'"):
        component.build_defs(context=None)


def test_single_run_backfill_fetches_one_request_per_day_and_concatenates(mod, monkeypatch):
    """Real bug before the fix: the per-key logic read exactly ONE
    context.partition_key (gated on context.has_partition_key), which
    doesn't exist for a multi-partition single_run backfill --
    has_partition_key is False for a range, so {partition_key} templated to
    an empty string, hitting a URL with no date segment at all. Confirmed
    fixed: a 3-day single_run backfill now issues one request per day in the
    range (via a per-key context shim reusing the exact same per-key logic)
    and concatenates exactly those records."""
    fake = FakeRestServer({
        "2024-01-02": [{"id": 1}, {"id": 2}],
        "2024-01-03": [{"id": 3}],
        "2024-01-04": [{"id": 4}, {"id": 5}, {"id": 6}],
    })
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
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
    assert len(fake.urls) == 3


def test_single_run_backfill_sink_partition_dim_column_is_assigned_per_row(mod, monkeypatch, tmp_path):
    """Real bug this fix also had to cover: a sink's partition_dim_columns
    mapped to dim_name='partition_key' called str(context.partition_key)
    directly, which RAISES for a range (has_partition_key is False).
    Confirmed fixed: each row gets the partition key of the chunk it came
    from, in concatenation order -- verified via a real local DuckDB
    connection (the sink's actual register()/execute() fast path, not
    mocked). Uses a file-backed DB (not :memory:) since the sink's
    `with _acquire() as conn:` closes the connection it's handed on exit."""
    fake = FakeRestServer({
        "2024-01-02": [{"id": 1}, {"id": 2}],
        "2024-01-03": [{"id": 3}],
    })
    db_path = str(tmp_path / "sink.duckdb")
    conn = duckdb.connect(db_path)
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
        sinks=[{
            "kind": "table",
            "resource_key": "duckdb_sink",
            "table": "sales_sink",
            "partition_dim_columns": {"sale_date": "partition_key"},
        }],
    )
    result = _materialize(
        mod, component, fake, monkeypatch,
        resources={"duckdb_sink": FakeDuckDBResource(conn)},
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-03",
        },
    )
    assert result.success
    verify_conn = duckdb.connect(db_path)
    rows = verify_conn.execute("SELECT id, sale_date FROM sales_sink ORDER BY id").fetchall()
    verify_conn.close()
    assert rows == [(1, "2024-01-02"), (2, "2024-01-02"), (3, "2024-01-03")]
