"""Committed tests for ODataIngestionComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.

Mocks ONLY requests.get (keyed by the resolved `$filter` query option) --
every other behavior (partitions_def construction, $filter templating,
pagination, v2 `{d: {results, __next}}` parsing, DataFrame building) is
exercised for real through dg.materialize().

This asset returns Output(value=df) -- it IS routed through Dagster's IO
manager, so a single_run backfill here has the multi-partition-output IO
manager constraint documented on the field -- tests use
dg.InMemoryIOManager() + the `dagster/asset_partition_range_*` run tags a
real single_run backfill sets.
"""
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeODataServer:
    """Maps an exact `$filter` value to canned v2 records; records every
    requested URL."""

    def __init__(self, filter_to_records: dict):
        self.filter_to_records = filter_to_records
        self.urls: list = []

    def handle_get(self, url, auth=None, headers=None, **kwargs):
        self.urls.append(url)
        qs = parse_qs(urlparse(url).query)
        filt = qs.get("$filter", [None])[0]
        records = self.filter_to_records.get(filt, [])
        resp = MagicMock()
        resp.raise_for_status = lambda: None
        resp.json.return_value = {"d": {"results": records}}
        return resp


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="business_partners",
        service_url="https://odata.example.com/service",
        entity_set="A_BusinessPartner",
        auth_type="none",
        filter="CreationDate eq '{partition_key}'",
    )
    kwargs.update(overrides)
    return mod.ODataIngestionComponent(**kwargs)


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
    fake = FakeODataServer({"CreationDate eq '2024-01-02'": [{"Id": "1"}, {"Id": "2"}]})
    component = _make_component(
        mod, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(mod, component, fake, monkeypatch, partition_key="2024-01-02")
    assert result.success
    df = result.output_for_node("business_partners")
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


def test_single_run_backfill_fetches_one_request_per_day_and_concatenates(mod, monkeypatch):
    """Real bug before the fix: the asset substituted exactly ONE
    context.partition_key (gated on context.has_partition_key) into
    $filter, which doesn't exist for a multi-partition single_run backfill
    -- has_partition_key is False for a range, so partition_key silently
    became None and the unresolved '{partition_key}' literal was sent as
    the literal $filter value, matching nothing. Confirmed fixed: a 3-day
    single_run backfill now issues one request per day in the range and
    concatenates exactly those records."""
    fake = FakeODataServer({
        "CreationDate eq '2024-01-02'": [{"Id": "1"}, {"Id": "2"}],
        "CreationDate eq '2024-01-03'": [{"Id": "3"}],
        "CreationDate eq '2024-01-04'": [{"Id": "4"}, {"Id": "5"}, {"Id": "6"}],
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
    df = result.output_for_node("business_partners")
    assert len(df) == 6
    assert sorted(df["Id"].tolist()) == ["1", "2", "3", "4", "5", "6"]
    assert len(fake.urls) == 3
