"""Committed tests for GainsightCompanyIngestionComponent.

Mocks ONLY the Gainsight HTTP call (via a fake resource object exposing
`.post(path, json)`, matching GainsightResource's real interface) -- every
other behavior (partitions_def construction, DataFrame building, preview
metadata, pagination loop logic, where-filter merging, error handling) is
exercised for real through dg.materialize().
"""
from typing import Any, Dict, List, Optional

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeGainsightResource:
    """Returns one queued page per `.post()` call; records every call made."""

    def __init__(self, pages: Optional[List[List[Dict[str, Any]]]] = None):
        self.pages = list(pages) if pages is not None else []
        self.calls: List[Dict[str, Any]] = []

    def post(self, path: str, json: Optional[Dict[str, Any]] = None) -> dict:
        self.calls.append({"path": path, "json": json})
        data = self.pages.pop(0) if self.pages else []
        return {"result": True, "data": data, "requestId": "test-request-id"}


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="gainsight_companies", resource_name="gainsight_resource")
    kwargs.update(overrides)
    return mod.GainsightCompanyIngestionComponent(**kwargs)


def _materialize(component, fake_resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={component.resource_name: fake_resource},
        partition_key=partition_key,
    )


def test_basic_fetch_single_short_page(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1", "Name": "Acme"}]])
    component = _make_component(mod, limit=50, page_size=50)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("gainsight_companies"))
    assert len(df) == 1
    assert df.iloc[0]["Name"] == "Acme"
    assert len(fake.calls) == 1  # short page (1 < 50) stops after first call


def test_pagination_across_multiple_full_pages(mod):
    fake = FakeGainsightResource(pages=[
        [{"Gsid": "1"}, {"Gsid": "2"}],
        [{"Gsid": "3"}, {"Gsid": "4"}],
        [{"Gsid": "5"}],
    ])
    component = _make_component(mod, limit=5, page_size=2)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("gainsight_companies"))
    assert len(df) == 5
    assert list(df["Gsid"]) == ["1", "2", "3", "4", "5"]
    assert len(fake.calls) == 3
    # offsets must walk forward by records actually returned
    assert fake.calls[0]["json"]["offset"] == 0
    assert fake.calls[1]["json"]["offset"] == 2
    assert fake.calls[2]["json"]["offset"] == 4


def test_empty_result_returns_empty_dataframe(mod):
    fake = FakeGainsightResource(pages=[[]])
    component = _make_component(mod)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("gainsight_companies"))
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["row_count"].value == 0


def test_limit_truncates_even_if_api_overreturns(mod):
    """Safety net: if the upstream API ever ignores our requested page size
    and returns more records than asked, the component must still cap the
    final DataFrame at `limit`."""
    fake = FakeGainsightResource(pages=[[{"Gsid": str(i)} for i in range(10)]])
    component = _make_component(mod, limit=3, page_size=50)
    result = _materialize(component, fake)
    assert result.success
    df = result.asset_value(dg.AssetKey("gainsight_companies"))
    assert len(df) == 3


def test_time_based_partition_builds_where_filter(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1"}]])
    component = _make_component(
        mod,
        partition_type="daily",
        partition_start="2024-01-01",
        date_field="Last_Modified_Date",
    )
    result = _materialize(component, fake, partition_key="2024-01-02")
    assert result.success
    body = fake.calls[0]["json"]
    conditions = body["where"]["conditions"]
    fields_and_ops = {(c["field"], c["operator"]): c["value"] for c in conditions}
    assert ("Last_Modified_Date", "GTE") in fields_and_ops
    assert ("Last_Modified_Date", "LTE") in fields_and_ops
    assert fields_and_ops[("Last_Modified_Date", "GTE")].startswith("2024-01-02")
    assert fields_and_ops[("Last_Modified_Date", "LTE")].startswith("2024-01-03")


def test_static_modified_after_before_builds_where_filter(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1"}]])
    component = _make_component(
        mod,
        modified_after="2026-01-01T00:00:00Z",
        modified_before="2026-02-01T00:00:00Z",
    )
    result = _materialize(component, fake)
    assert result.success
    body = fake.calls[0]["json"]
    conditions = body["where"]["conditions"]
    assert {"field": "Last_Modified_Date", "operator": "GTE", "value": "2026-01-01T00:00:00Z"} in conditions
    assert {"field": "Last_Modified_Date", "operator": "LTE", "value": "2026-02-01T00:00:00Z"} in conditions


def test_select_fields_passed_through_to_request(mod):
    fake = FakeGainsightResource(pages=[[{"Name": "Acme", "Csm": "Jane"}]])
    component = _make_component(mod, select_fields=["Name", "Csm"])
    result = _materialize(component, fake)
    assert result.success
    assert fake.calls[0]["json"]["select"] == ["Name", "Csm"]


def test_preview_metadata_included_when_enabled(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1", "Name": "Acme"}]])
    component = _make_component(mod, include_preview_metadata=True)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" in meta
    assert "Acme" in meta["preview"].value


def test_preview_metadata_excluded_when_disabled(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1", "Name": "Acme"}]])
    component = _make_component(mod, include_preview_metadata=False)
    result = _materialize(component, fake)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" not in meta


def test_custom_resource_name(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1"}]])
    component = _make_component(mod, resource_name="my_gainsight")
    result = _materialize(component, fake)
    assert result.success


def test_custom_object_name_builds_correct_path(mod):
    fake = FakeGainsightResource(pages=[[{"Id": "r1"}]])
    component = _make_component(mod, object_name="Relationship")
    result = _materialize(component, fake)
    assert result.success
    assert fake.calls[0]["path"] == "v1/data/objects/query/Relationship"


def test_where_filter_merged_with_date_filter_and_expression_preserved(mod):
    fake = FakeGainsightResource(pages=[[{"Gsid": "1"}]])
    component = _make_component(
        mod,
        where_filter={
            "conditions": [{"field": "Status", "operator": "EQ", "value": "Active"}],
            "expression": "A",
        },
        modified_after="2026-01-01T00:00:00Z",
    )
    result = _materialize(component, fake)
    assert result.success
    where_clause = fake.calls[0]["json"]["where"]
    assert {"field": "Status", "operator": "EQ", "value": "Active"} in where_clause["conditions"]
    assert {"field": "Last_Modified_Date", "operator": "GTE", "value": "2026-01-01T00:00:00Z"} in where_clause["conditions"]
    assert where_clause["expression"] == "A"
