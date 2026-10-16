"""Committed tests for OAuthRestIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Mocks ONLY requests.get (keyed by the resolved `date` query param) -- every
other behavior (partitions_def construction, query_params templating,
pagination, DataFrame building, preview metadata) is exercised for real
through dg.materialize().

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


class FakeOAuthServer:
    """Maps a `date` query param value to canned records; records every
    request's params."""

    def __init__(self, date_to_records: dict):
        self.date_to_records = date_to_records
        self.calls: list = []

    def handle_get(self, url, params=None, headers=None, **kwargs):
        self.calls.append(dict(params or {}))
        records = self.date_to_records.get((params or {}).get("date"), [])
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"Items": records}
        return resp


def _make_component(mod, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_OAUTH_TOKEN", "test-token")
    kwargs = dict(
        asset_name="daily_sales",
        api_url="https://api.example.com/sales",
        auth_token_env_var="FAKE_OAUTH_TOKEN",
        records_path="Items",
        query_params={"date": "{partition_key}"},
    )
    kwargs.update(overrides)
    return mod.OAuthRestIngestionComponent(**kwargs)


def _materialize(mod, component, fake, monkeypatch, partition_key=None, tags=None):
    monkeypatch.setattr(mod.requests, "get", fake.handle_get)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_fetches_one_days_records(mod, monkeypatch):
    fake = FakeOAuthServer({"2024-01-02": [{"id": 1}, {"id": 2}]})
    component = _make_component(
        mod, monkeypatch, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(mod, component, fake, monkeypatch, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("daily_sales")
    assert len(df) == 2
    assert len(fake.calls) == 1


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


def test_single_run_backfill_fetches_one_request_per_day_and_concatenates(mod, monkeypatch):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into
    query_params, which doesn't exist for a multi-partition single_run
    backfill -- has_partition_key is False for a range, so partition_key
    silently became None and the unresolved '{partition_key}' literal was
    sent as the literal `date` param, matching nothing. Confirmed fixed: a
    3-day single_run backfill now issues one request per day in the range
    and concatenates exactly those records."""
    fake = FakeOAuthServer({
        "2024-01-02": [{"id": 1}, {"id": 2}],
        "2024-01-03": [{"id": 3}],
        "2024-01-04": [{"id": 4}, {"id": 5}, {"id": 6}],
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
    assert len(fake.calls) == 3
