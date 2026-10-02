"""Committed regression tests for ConfluencePageUpsertComponent.

The real `ConfluenceResource` (which would make HTTP calls to Confluence
Cloud) is never used here -- `FakeConfluenceResource` (conftest.py) stands
in for the one external, paid-API boundary, while everything this
component actually owns -- dual source resolution, validation, title/body
handling, batching, create-vs-update branching, error aggregation, and
metadata -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeConfluenceResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_pages", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"confluence": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ConfluencePageUpsertComponent(
            asset_name="x",
            space_id="123",
            fields_map={"page_title": "title"},
            body_column="body",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ConfluencePageUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            space_id="123",
            fields_map={"page_title": "title"},
            body_column="body",
        ).build_defs(context=None)


def test_title_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="title"):
        mod.ConfluencePageUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            space_id="123",
            fields_map={"page_name": "not_title"},
            body_column="body",
        ).build_defs(context=None)


# --- empty / capping / skipping --------------------------------------------

def test_empty_upstream_returns_zero(mod):
    df = pd.DataFrame({"page_title": [], "body": []})
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="123",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["rows_upserted"] == 0


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame(
        {
            "page_title": [f"Page {i}" for i in range(10)],
            "body": [f"body {i}" for i in range(10)],
        }
    )
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="123",
        fields_map={"page_title": "title"},
        body_column="body",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["rows_upserted"] == 3
    assert len(resource.create_calls) == 3


def test_rows_with_blank_title_are_skipped_and_counted(mod):
    df = pd.DataFrame(
        {
            "page_title": ["Release Notes", None, "   "],
            "body": ["a", "b", "c"],
        }
    )
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="123",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["rows_upserted"] == 1
    assert out["rows_skipped_blank_title"] == 2


def test_missing_required_column_raises_failure(mod):
    df = pd.DataFrame({"page_title": ["Release Notes"]})  # no 'body' column
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="123",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)


# --- create vs update branching, including version-number-plus-one --------

def test_create_then_update_same_title_bumps_version_by_one(mod):
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="space-1",
        fields_map={"page_title": "title"},
        body_column="body",
    )

    # First run: page doesn't exist yet -> created at version 1.
    df1 = pd.DataFrame({"page_title": ["Release Notes"], "body": ["v1 content"]})
    result1 = _materialize(component, df1, resource)
    assert result1.success
    assert len(resource.create_calls) == 1
    assert len(resource.update_calls) == 0
    out1 = _metadata_for(result1, "confluence_page_upsert_out")
    assert out1["rows_created"] == 1
    assert out1["rows_updated"] == 0

    page_id = next(iter(resource.pages))
    assert resource.pages[page_id]["version"]["number"] == 1

    # Second run: same title now exists -> update, version bumps from 1 to 2.
    df2 = pd.DataFrame({"page_title": ["Release Notes"], "body": ["v2 content"]})
    result2 = _materialize(component, df2, resource)
    assert result2.success
    assert len(resource.create_calls) == 1  # unchanged
    assert len(resource.update_calls) == 1
    update_call = resource.update_calls[0]
    assert update_call["current_version"] == 1
    assert update_call["new_version"] == 2  # exactly current + 1
    assert resource.pages[page_id]["version"]["number"] == 2

    out2 = _metadata_for(result2, "confluence_page_upsert_out")
    assert out2["rows_created"] == 0
    assert out2["rows_updated"] == 1


def test_wrap_body_as_html_default_wraps_plain_text_in_p_tags(mod):
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="space-1",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    df = pd.DataFrame({"page_title": ["Release Notes"], "body": ["hello & welcome"]})
    _materialize(component, df, resource)
    sent_body = resource.create_calls[0]["body_storage"]
    assert sent_body == "<p>hello &amp; welcome</p>"


def test_wrap_body_as_html_false_passes_storage_xhtml_through(mod):
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="space-1",
        fields_map={"page_title": "title"},
        body_column="body",
        wrap_body_as_html=False,
    )
    raw_storage = "<table><tbody><tr><td>cell</td></tr></tbody></table>"
    df = pd.DataFrame({"page_title": ["Release Notes"], "body": [raw_storage]})
    _materialize(component, df, resource)
    assert resource.create_calls[0]["body_storage"] == raw_storage


# --- error aggregation -------------------------------------------------

def test_error_in_one_row_is_aggregated_not_fatal(mod):
    df = pd.DataFrame(
        {
            "page_title": ["Good Page", "Bad Page", "Another Good Page"],
            "body": ["a", "b", "c"],
        }
    )
    resource = FakeConfluenceResource(fail_titles={"Bad Page"})
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="space-1",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    result = _materialize(component, df, resource)
    assert result.success  # errors are aggregated, not fatal to the run
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["rows_created"] == 2
    assert out["rows_errored"] == 1
    assert out["rows_upserted"] == 2
    assert "Bad Page" in out["first_errors"][0]


# --- inline source mode --------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        source={
            "kind": "inline",
            "rows": [
                {"page_title": "Page A", "body": "content a"},
                {"page_title": "Page B", "body": "content b"},
            ],
        },
        space_id="space-1",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"confluence": resource})
    assert result.success
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["rows_upserted"] == 2
    assert len(resource.create_calls) == 2


# --- metadata --------------------------------------------------------------

def test_metadata_field_values(mod):
    df = pd.DataFrame({"page_title": ["Release Notes"], "body": ["content"]})
    resource = FakeConfluenceResource()
    component = mod.ConfluencePageUpsertComponent(
        asset_name="confluence_page_upsert_out",
        upstream_asset_key="upstream_pages",
        space_id="space-42",
        fields_map={"page_title": "title"},
        body_column="body",
    )
    result = _materialize(component, df, resource)
    out = _metadata_for(result, "confluence_page_upsert_out")
    assert out["confluence_space_id"] == "space-42"
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0
    assert out["rows_upserted"] == 1
    assert out["rows_errored"] == 0
    assert out["rows_skipped_blank_title"] == 0
