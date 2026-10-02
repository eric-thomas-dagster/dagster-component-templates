"""Committed regression tests for SalesloftPeopleIngestionComponent.

The real Salesloft REST API is never called -- `FakeSalesloftResource`
(conftest.py) stands in for the one external, paid-API boundary
(`resource.get(path, params)`), while everything this component actually
owns -- flat DataFrame construction, page-number pagination via
`metadata.paging.next_page`, partitions_def construction, filter-param
translation, limit enforcement, and preview metadata -- is exercised for
real via `dg.materialize`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeSalesloftResource, load_component_module, make_person


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={"salesloft_resource": resource},
        partition_key=partition_key,
    )


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- partitions_def construction -----------------------------------------

def test_no_partition_type_means_unpartitioned(mod):
    component = mod.SalesloftPeopleIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert asset_def.partitions_def is None


def test_weekly_partition_type_builds_weekly_partitions_def(mod):
    component = mod.SalesloftPeopleIngestionComponent(
        asset_name="x", partition_type="weekly", partition_start="2026-01-01"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert isinstance(asset_def.partitions_def, dg.WeeklyPartitionsDefinition)


def test_dynamic_partition_without_name_raises(mod):
    component = mod.SalesloftPeopleIngestionComponent(asset_name="x", partition_type="dynamic")
    with pytest.raises(ValueError, match="requires dynamic_partition_name"):
        component.build_defs(context=None)


# --- end-to-end against the fake resource --------------------------------

def test_single_page_materializes_flat_dataframe(mod):
    page1 = {
        "data": [
            make_person("1", first_name="Jane", last_name="Doe", email_address="jane@example.com"),
            make_person("2", first_name="Bob", last_name="Smith", email_address="bob@example.com"),
        ],
        "metadata": {"paging": {"per_page": 100, "current_page": 1, "next_page": None, "total_pages": 1}},
    }
    resource = FakeSalesloftResource({1: page1})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 2
    assert len(resource.calls) == 1


def test_data_items_are_flat_no_attribute_unwrapping_needed(mod):
    # Unlike Outreach's JSON:API shape, Salesloft rows land directly as
    # top-level DataFrame columns -- no nested "attributes" key to unwrap.
    page1 = {
        "data": [make_person("1", first_name="Jane")],
        "metadata": {"paging": {"next_page": None}},
    }
    resource = FakeSalesloftResource({1: page1})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people")
    result = _materialize(component, resource)
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 1
    assert "preview" in out
    assert "first_name" in out["preview"]


def test_pagination_follows_next_page_until_null(mod):
    page1 = {
        "data": [make_person("1", first_name="A")],
        "metadata": {"paging": {"next_page": 2}},
    }
    page2 = {
        "data": [make_person("2", first_name="B")],
        "metadata": {"paging": {"next_page": None}},
    }
    resource = FakeSalesloftResource({1: page1, 2: page2})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people", limit=100)
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 2
    assert len(resource.calls) == 2
    assert resource.calls[0]["params"]["page"] == 1
    assert resource.calls[1]["params"]["page"] == 2


def test_limit_stops_pagination_early(mod):
    page1 = {
        "data": [make_person("1", first_name="A"), make_person("2", first_name="B")],
        "metadata": {"paging": {"next_page": 2}},
    }
    page2 = {
        "data": [make_person("3", first_name="C")],
        "metadata": {"paging": {"next_page": None}},
    }
    resource = FakeSalesloftResource({1: page1, 2: page2})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people", limit=2)
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 2
    # Limit satisfied by page 1 alone -- page 2 is never fetched.
    assert len(resource.calls) == 1


def test_empty_result_returns_empty_dataframe_and_zero_row_count(mod):
    resource = FakeSalesloftResource({1: {"data": [], "metadata": {"paging": {"next_page": None}}}})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 0
    assert "preview" not in out


def test_missing_paging_metadata_stops_after_one_page(mod):
    # A malformed / minimal response with no "metadata" key at all should
    # not crash -- it should just stop (no next_page to follow).
    resource = FakeSalesloftResource({1: {"data": [make_person("1", first_name="A")]}})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "salesloft_people")
    assert out["row_count"] == 1
    assert len(resource.calls) == 1


def test_preview_metadata_omitted_when_disabled(mod):
    page1 = {"data": [make_person("1", first_name="A")], "metadata": {"paging": {"next_page": None}}}
    resource = FakeSalesloftResource({1: page1})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people", include_preview_metadata=False)
    result = _materialize(component, resource)
    out = _metadata_for(result, "salesloft_people")
    assert "preview" not in out


def test_updated_after_and_before_translate_to_bracket_filter_params(mod):
    resource = FakeSalesloftResource({1: {"data": [], "metadata": {}}})
    component = mod.SalesloftPeopleIngestionComponent(
        asset_name="salesloft_people",
        updated_after="2026-06-01T00:00:00Z",
        updated_before="2026-07-01T00:00:00Z",
    )
    _materialize(component, resource)
    params = resource.calls[0]["params"]
    assert params["updated_at[gte]"] == "2026-06-01T00:00:00Z"
    assert params["updated_at[lte]"] == "2026-07-01T00:00:00Z"


def test_no_time_filter_omits_filter_params(mod):
    resource = FakeSalesloftResource({1: {"data": [], "metadata": {}}})
    component = mod.SalesloftPeopleIngestionComponent(asset_name="salesloft_people")
    _materialize(component, resource)
    params = resource.calls[0]["params"]
    assert not any(k.startswith("updated_at[") for k in params)


def test_time_partition_window_overrides_static_config(mod):
    resource = FakeSalesloftResource({1: {"data": [], "metadata": {}}})
    component = mod.SalesloftPeopleIngestionComponent(
        asset_name="salesloft_people",
        updated_after="2000-01-01T00:00:00Z",  # should be ignored in favor of the partition window
        updated_before="2000-01-02T00:00:00Z",
        partition_type="daily",
        partition_start="2026-01-01",
    )
    _materialize(component, resource, partition_key="2026-01-05")
    params = resource.calls[0]["params"]
    assert params["updated_at[gte]"] == "2026-01-05T00:00:00Z"
    assert params["updated_at[lte]"] == "2026-01-06T00:00:00Z"


def test_default_kinds_tag_salesloft_and_python(mod):
    component = mod.SalesloftPeopleIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(dg.AssetKey("x"))
    assert "dagster/kind/salesloft" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_required_resource_keys_includes_configured_resource_name(mod):
    component = mod.SalesloftPeopleIngestionComponent(asset_name="x", resource_name="my_salesloft")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert "my_salesloft" in asset_def.required_resource_keys
