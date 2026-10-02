"""Committed regression tests for SigmaWorkbookIngestionComponent.

The real Sigma REST API is never called -- `FakeSigmaResource`
(conftest.py) stands in for the one external, paid-API boundary
(`resource.get(path, params)`), while everything this component actually
owns -- workbook/element pagination, input-table filtering, partitions_def
construction, limit enforcement, and preview metadata -- is exercised for
real via `dg.materialize`.
"""
import dagster as dg
import pytest

from .conftest import FakeSigmaResource, load_component_module, make_element, make_workbook


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource, partition_key=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize(
        [asset_def],
        resources={"sigma_resource": resource},
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
    component = mod.SigmaWorkbookIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert asset_def.partitions_def is None


def test_weekly_partition_type_builds_weekly_partitions_def(mod):
    component = mod.SigmaWorkbookIngestionComponent(
        asset_name="x", partition_type="weekly", partition_start="2026-01-01"
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert isinstance(asset_def.partitions_def, dg.WeeklyPartitionsDefinition)


def test_dynamic_partition_without_name_raises(mod):
    component = mod.SigmaWorkbookIngestionComponent(asset_name="x", partition_type="dynamic")
    with pytest.raises(ValueError, match="requires dynamic_partition_name"):
        component.build_defs(context=None)


# --- end-to-end against the fake resource --------------------------------

def test_single_workbook_with_elements(mod):
    responses = {
        ("v2/workbooks", None): {
            "entries": [make_workbook("wb1", name="Sales Dashboard", path="/Sales")],
            "nextPage": None,
        },
        ("v2/workbooks/wb1/elements", None): {
            "entries": [
                make_element("el1", "table", name="Revenue Table", columns=["date", "amount"]),
                make_element("el2", "viz", name="Revenue Chart", vizualizationType="line"),
            ],
            "nextPage": None,
        },
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(asset_name="sigma_workbook_catalog")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["row_count"] == 2
    assert out["workbook_count"] == 1
    assert out["elements_fetched"] == 2
    assert out["input_table_count"] == 0


def test_input_table_elements_are_flagged(mod):
    responses = {
        ("v2/workbooks", None): {"entries": [make_workbook("wb1")], "nextPage": None},
        ("v2/workbooks/wb1/elements", None): {
            "entries": [
                make_element("el1", "input-table", name="Forecast Inputs", columns=["region", "forecast"]),
                make_element("el2", "table", name="Actuals"),
            ],
            "nextPage": None,
        },
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(asset_name="sigma_workbook_catalog")
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["input_table_count"] == 1


def test_input_tables_only_drops_non_input_table_rows_and_empty_workbooks(mod):
    responses = {
        ("v2/workbooks", None): {
            "entries": [make_workbook("wb1"), make_workbook("wb2")],
            "nextPage": None,
        },
        ("v2/workbooks/wb1/elements", None): {
            "entries": [
                make_element("el1", "input-table", name="Forecast Inputs"),
                make_element("el2", "table", name="Actuals"),
            ],
            "nextPage": None,
        },
        # wb2 has zero input tables -- should contribute no rows at all.
        ("v2/workbooks/wb2/elements", None): {
            "entries": [make_element("el3", "viz", name="Chart")],
            "nextPage": None,
        },
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(
        asset_name="sigma_workbook_catalog", input_tables_only=True
    )
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["row_count"] == 1
    assert out["input_table_count"] == 1
    assert "wb2" not in out["preview"]


def test_include_elements_false_emits_one_row_per_workbook(mod):
    responses = {
        ("v2/workbooks", None): {
            "entries": [make_workbook("wb1", name="A"), make_workbook("wb2", name="B")],
            "nextPage": None,
        },
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(
        asset_name="sigma_workbook_catalog", include_elements=False
    )
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["row_count"] == 2
    assert out["elements_fetched"] == 0
    # Elements endpoint should never have been called.
    assert all(c["path"] == "v2/workbooks" for c in resource.calls)


def test_workbook_pagination_follows_next_page(mod):
    responses = {
        ("v2/workbooks", None): {"entries": [make_workbook("wb1")], "nextPage": "p2"},
        ("v2/workbooks", "p2"): {"entries": [make_workbook("wb2")], "nextPage": None},
        ("v2/workbooks/wb1/elements", None): {"entries": [], "nextPage": None},
        ("v2/workbooks/wb2/elements", None): {"entries": [], "nextPage": None},
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(asset_name="sigma_workbook_catalog")
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["workbook_count"] == 2


def test_workbook_limit_caps_total_workbooks(mod):
    responses = {
        ("v2/workbooks", None): {
            "entries": [make_workbook("wb1"), make_workbook("wb2"), make_workbook("wb3")],
            "nextPage": None,
        },
        ("v2/workbooks/wb1/elements", None): {"entries": [], "nextPage": None},
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(
        asset_name="sigma_workbook_catalog", workbook_limit=1
    )
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["workbook_count"] == 1


def test_empty_catalog_returns_empty_dataframe(mod):
    resource = FakeSigmaResource({("v2/workbooks", None): {"entries": [], "nextPage": None}})
    component = mod.SigmaWorkbookIngestionComponent(asset_name="sigma_workbook_catalog")
    result = _materialize(component, resource)
    assert result.success
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert out["row_count"] == 0
    assert "preview" not in out


def test_preview_metadata_omitted_when_disabled(mod):
    responses = {
        ("v2/workbooks", None): {"entries": [make_workbook("wb1")], "nextPage": None},
        ("v2/workbooks/wb1/elements", None): {
            "entries": [make_element("el1", "table")],
            "nextPage": None,
        },
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(
        asset_name="sigma_workbook_catalog", include_preview_metadata=False
    )
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert "preview" not in out


def test_default_kinds_tag_sigma_and_python(mod):
    component = mod.SigmaWorkbookIngestionComponent(asset_name="x")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    spec = asset_def.get_asset_spec(dg.AssetKey("x"))
    assert "dagster/kind/sigma" in spec.tags
    assert "dagster/kind/python" in spec.tags


def test_required_resource_keys_includes_configured_resource_name(mod):
    component = mod.SigmaWorkbookIngestionComponent(asset_name="x", resource_name="my_sigma")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assert "my_sigma" in asset_def.required_resource_keys


def test_workbook_level_audit_fields_are_surfaced(mod):
    responses = {
        ("v2/workbooks", None): {
            "entries": [
                make_workbook(
                    "wb1",
                    name="Sales",
                    createdBy="user1",
                    updatedBy="user2",
                    createdAt="2026-01-01T00:00:00Z",
                    updatedAt="2026-02-01T00:00:00Z",
                    latestVersion=3,
                )
            ],
            "nextPage": None,
        },
        ("v2/workbooks/wb1/elements", None): {"entries": [], "nextPage": None},
    }
    resource = FakeSigmaResource(responses)
    component = mod.SigmaWorkbookIngestionComponent(asset_name="sigma_workbook_catalog")
    result = _materialize(component, resource)
    out = _metadata_for(result, "sigma_workbook_catalog")
    assert "user1" in out["preview"]
    assert "user2" in out["preview"]
