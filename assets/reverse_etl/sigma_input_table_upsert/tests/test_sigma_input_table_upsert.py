"""Committed regression tests for SigmaInputTableUpsertComponent.

The real Sigma REST API is never called -- `FakeSigmaResource`
(conftest.py) stands in for the one external, paid-API boundary
(`resource.get()` / `resource.post()`), while everything this component
actually owns -- dual source resolution, validation, fields_map row
building, schema pre-flight, chunking, and metadata -- is exercised for
real via `dg.materialize`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeSigmaResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_forecast", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"sigma_resource": resource})


# --- validation --------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.SigmaInputTableUpsertComponent(
            asset_name="x",
            workbook_id="wb1",
            sequence_id="seq1",
            fields_map={"region": "Region"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.SigmaInputTableUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            workbook_id="wb1",
            sequence_id="seq1",
            fields_map={"region": "Region"},
        ).build_defs(context=None)


def test_invalid_write_mode_raises(mod):
    with pytest.raises(ValueError, match="write_mode must be"):
        mod.SigmaInputTableUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            workbook_id="wb1",
            sequence_id="seq1",
            fields_map={"region": "Region"},
            write_mode="upsert",
        ).build_defs(context=None)


def test_empty_fields_map_raises(mod):
    with pytest.raises(ValueError, match="fields_map must map at least one"):
        mod.SigmaInputTableUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            workbook_id="wb1",
            sequence_id="seq1",
            fields_map={},
        ).build_defs(context=None)


def test_rows_per_request_over_2000_rejected_by_pydantic(mod):
    with pytest.raises(Exception):
        mod.SigmaInputTableUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            workbook_id="wb1",
            sequence_id="seq1",
            fields_map={"region": "Region"},
            rows_per_request=2001,
        )


# --- end-to-end against the fake resource --------------------------------

def test_basic_write_posts_mapped_rows_to_webhook(mod):
    df = pd.DataFrame({"region": ["us", "eu"], "forecast": [100, 200]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region", "forecast": "Forecast"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.post_calls) == 1
    call = resource.post_calls[0]
    assert call["path"] == "v2/webhooks/wb1/seq1"
    assert call["json_body"] == {
        "rows": [
            {"Region": "us", "Forecast": 100},
            {"Region": "eu", "Forecast": 200},
        ]
    }

    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["rows_submitted"] == 2
    assert out["workbook_id"] == "wb1"
    assert out["sequence_id"] == "seq1"
    assert out["write_mode"] == "insert"
    assert out["api_requests"] == 1
    assert out["trace_ids"] == ["trace-1"]


def test_custom_rows_parameter_name_and_static_parameters(mod):
    df = pd.DataFrame({"region": ["us"]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        rows_parameter_name="new_rows",
        static_parameters={"batch_marker": "nightly"},
        fields_map={"region": "Region"},
    )
    # Schema must declare both rows_parameter_name and static_parameters keys.
    resource._schema_response = {"variables": {"new_rows": {}, "batch_marker": {}}}
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.post_calls[0]["json_body"] == {
        "new_rows": [{"Region": "us"}],
        "batch_marker": "nightly",
    }


def test_schema_validation_fails_fast_on_unknown_parameter(mod):
    df = pd.DataFrame({"region": ["us"]})
    resource = FakeSigmaResource(schema_response={"variables": {"something_else": {}}})
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
    )
    with pytest.raises(dg.Failure, match="doesn't declare parameter"):
        _materialize(component, df, resource)
    # The webhook itself must never be called once the pre-flight fails.
    assert resource.post_calls == []


def test_schema_validation_can_be_disabled(mod):
    df = pd.DataFrame({"region": ["us"]})
    resource = FakeSigmaResource(schema_response={"variables": {"something_else": {}}})
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
        validate_schema_before_write=False,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.get_calls == []
    assert len(resource.post_calls) == 1


def test_missing_column_raises_failure(mod):
    df = pd.DataFrame({"other_col": ["us"]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)


def test_empty_dataframe_makes_no_api_call(mod):
    df = pd.DataFrame({"region": []})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.get_calls == []
    assert resource.post_calls == []
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["rows_submitted"] == 0


def test_all_null_rows_skipped_and_no_api_call(mod):
    df = pd.DataFrame({"region": [None, None]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.post_calls == []
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["rows_submitted"] == 0
    assert out["rows_skipped_empty"] == 2


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"region": [f"r{i}" for i in range(10)]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["rows_submitted"] == 3


def test_rows_chunked_at_rows_per_request(mod):
    df = pd.DataFrame({"region": [f"r{i}" for i in range(5)]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
        rows_per_request=2,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.post_calls) == 3
    assert [len(c["json_body"]["rows"]) for c in resource.post_calls] == [2, 2, 1]
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["api_requests"] == 3


def test_update_write_mode_is_metadata_only(mod):
    df = pd.DataFrame({"region": ["us"]})
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        upstream_asset_key="upstream_forecast",
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
        write_mode="update",
    )
    result = _materialize(component, df, resource)
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["write_mode"] == "update"
    # Request shape is identical regardless of write_mode.
    assert resource.post_calls[0]["json_body"] == {"rows": [{"Region": "us"}]}


def test_source_inline_mode(mod):
    resource = FakeSigmaResource()
    component = mod.SigmaInputTableUpsertComponent(
        asset_name="sigma_input_table_upsert_out",
        source={"kind": "inline", "rows": [{"region": "us"}, {"region": "eu"}]},
        workbook_id="wb1",
        sequence_id="seq1",
        fields_map={"region": "Region"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"sigma_resource": resource})
    assert result.success
    out = metadata_for(result, "sigma_input_table_upsert_out")
    assert out["rows_submitted"] == 2
