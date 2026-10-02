"""Committed regression tests for SmartsheetRowUpsertComponent.

The real Smartsheet HTTP API is never called here -- FakeSmartsheetResource
(conftest.py) stands in for the one external boundary, while everything
this component actually owns -- dual source resolution, validation,
row-building, blank-key skipping, batch capping, and metadata -- is
exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeSmartsheetResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_tasks", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"smartsheet": resource})


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
        mod.SmartsheetRowUpsertComponent(
            asset_name="x",
            sheet_id="123",
            key_column="Task ID",
            fields_map={"task_id": "Task ID"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.SmartsheetRowUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            sheet_id="123",
            key_column="Task ID",
            fields_map={"task_id": "Task ID"},
        ).build_defs(context=None)


def test_key_column_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not present in fields_map values"):
        mod.SmartsheetRowUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            sheet_id="123",
            key_column="Task ID",
            fields_map={"task_id": "Task Name"},
        ).build_defs(context=None)


# --- empty upstream ----------------------------------------------------------

def test_empty_upstream_upserts_nothing(mod):
    df = pd.DataFrame({"task_id": [], "name": []})
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_upserted"] == 0


# --- batch_size capping ------------------------------------------------------

def test_batch_size_caps_at_500_even_if_configured_higher(mod):
    df = pd.DataFrame({"task_id": [f"T{i}" for i in range(10)]})
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID"},
        batch_size=10000,
    )
    result = _materialize(component, df, resource)
    assert result.success
    # All 10 rows pass through -- the cap only matters once upstream exceeds 500.
    assert len(resource.upsert_calls[0]["rows"]) == 10


def test_batch_size_caps_rows_when_upstream_exceeds_cap(mod):
    df = pd.DataFrame({"task_id": [f"T{i}" for i in range(7)]})
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls[0]["rows"]) == 3
    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_upserted"] == 3


# --- blank key skipping -------------------------------------------------------

def test_rows_with_blank_key_value_are_skipped(mod):
    df = pd.DataFrame(
        {
            "task_id": ["T1", None, "   ", "T4"],
            "name": ["Alpha", "Beta", "Gamma", "Delta"],
        }
    )
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.upsert_calls[0]["rows"]) == 2
    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_skipped_blank_key"] == 2
    assert out["rows_upserted"] == 2


def test_all_rows_blank_key_upserts_nothing_and_does_not_call_resource(mod):
    df = pd.DataFrame({"task_id": [None, ""], "name": ["Alpha", "Beta"]})
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_skipped_blank_key"] == 2
    assert out["rows_upserted"] == 0


# --- create vs update branching ----------------------------------------------

def test_create_vs_update_branches_on_existing_key_match(mod):
    df = pd.DataFrame(
        {
            "task_id": ["T1", "T2", "T3"],
            "name": ["Alpha", "Beta", "Gamma"],
        }
    )
    # T2 already exists on the "sheet" -- everything else is new.
    resource = FakeSmartsheetResource(existing_rows_by_key={"T2": 42})
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.upsert_calls) == 1
    call = resource.upsert_calls[0]
    assert call["sheet_id"] == "123"
    assert call["key_column_title"] == "Task ID"
    sent_rows = {r["Task ID"]: r for r in call["rows"]}
    assert set(sent_rows) == {"T1", "T2", "T3"}

    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_created"] == 2
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 3
    assert out["sheet_id"] == "123"


# --- error aggregation ---------------------------------------------------

def test_resource_error_surfaces_as_failure(mod):
    df = pd.DataFrame({"task_id": ["T1"], "name": ["Alpha"]})
    resource = FakeSmartsheetResource(raise_on_upsert=RuntimeError("sheet locked"))
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    with pytest.raises(dg.Failure, match="sheet locked") as exc_info:
        _materialize(component, df, resource)
    message = str(exc_info.value)
    assert "123" in message
    # The attempt was made (and recorded) before the fake raised.
    assert len(resource.upsert_calls) == 1


# --- source: inline mode ------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeSmartsheetResource()
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        source={
            "kind": "inline",
            "rows": [{"task_id": "T1", "name": "Alpha"}, {"task_id": "T2", "name": "Beta"}],
        },
        sheet_id="123",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"smartsheet": resource})
    assert result.success
    out = _metadata_for(result, "smartsheet_out")
    assert out["rows_upserted"] == 2
    assert len(resource.upsert_calls[0]["rows"]) == 2


# --- metadata field values ------------------------------------------------

def test_metadata_fields_present_and_typed(mod):
    df = pd.DataFrame({"task_id": ["T1", "T2"], "name": ["Alpha", "Beta"]})
    resource = FakeSmartsheetResource(existing_rows_by_key={"T1": 7})
    component = mod.SmartsheetRowUpsertComponent(
        asset_name="smartsheet_out",
        upstream_asset_key="upstream_tasks",
        sheet_id="999888777",
        key_column="Task ID",
        fields_map={"task_id": "Task ID", "name": "Task Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "smartsheet_out")
    assert out == {
        "sheet_id": "999888777",
        "rows_created": 1,
        "rows_updated": 1,
        "rows_upserted": 2,
        "rows_skipped_blank_key": 0,
    }
