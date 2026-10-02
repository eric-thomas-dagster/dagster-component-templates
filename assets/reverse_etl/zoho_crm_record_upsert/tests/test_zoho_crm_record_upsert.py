"""Committed regression tests for ZohoCrmRecordUpsertComponent.

The real ZohoCrmResource (and its HTTP/OAuth boundary) is never imported --
a minimal FakeZohoCrmResource (conftest.py) stands in for the one external,
paid-API boundary (`.upsert()`), while everything this component actually
owns -- dual source resolution, duplicate_check_fields validation, chunking,
and metadata counting -- is exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeZohoCrmResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_leads", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"zoho_crm": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata directly."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- validation --------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZohoCrmRecordUpsertComponent(
            asset_name="x",
            module_api_name="Leads",
            duplicate_check_fields=["Email"],
            fields_map={"email": "Email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ZohoCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            module_api_name="Leads",
            duplicate_check_fields=["Email"],
            fields_map={"email": "Email"},
        ).build_defs(context=None)


def test_empty_duplicate_check_fields_raises(mod):
    with pytest.raises(ValueError, match="non-empty"):
        mod.ZohoCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            module_api_name="Leads",
            duplicate_check_fields=[],
            fields_map={"email": "Email"},
        ).build_defs(context=None)


def test_duplicate_check_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="not in fields_map values"):
        mod.ZohoCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            module_api_name="Leads",
            duplicate_check_fields=["Email"],
            fields_map={"first_name": "First_Name"},
        ).build_defs(context=None)


def test_multiple_duplicate_check_fields_all_must_be_mapped(mod):
    # Email is mapped, Mobile is not -> should still raise (only Mobile named).
    with pytest.raises(ValueError, match=r"\['Mobile'\]"):
        mod.ZohoCrmRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            module_api_name="Leads",
            duplicate_check_fields=["Email", "Mobile"],
            fields_map={"email": "Email"},
        ).build_defs(context=None)


# --- full asset body, against the fake resource ------------------------------

def test_insert_and_update_counted_from_response(mod):
    df = pd.DataFrame({
        "email": ["new@example.com", "existing@example.com", None],
        "first_name": ["New", "Existing", "Skipped"],
    })

    def response_for_chunk(call_index, records):
        return [
            {"code": "SUCCESS", "duplicate_field": None, "action": "insert",
             "status": "success", "message": "record added", "details": {"id": "1"}},
            {"code": "SUCCESS", "duplicate_field": "Email", "action": "update",
             "status": "success", "message": "record updated", "details": {"id": "2"}},
        ]

    resource = FakeZohoCrmResource(response_for_chunk=response_for_chunk)
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email", "first_name": "First_Name"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.upsert_calls) == 1
    call = resource.upsert_calls[0]
    assert call["module_api_name"] == "Leads"
    assert call["duplicate_check_fields"] == ["Email"]
    # Row with None email skipped -- only 2 records sent.
    assert len(call["records"]) == 2
    assert call["records"][0] == {"Email": "new@example.com", "First_Name": "New"}

    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1
    assert out["rows_upserted"] == 2
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_key"] == 1
    assert out["zoho_module"] == "Leads"
    assert out["duplicate_check_fields"] == ["Email"]


def test_error_status_counted_as_errored(mod):
    df = pd.DataFrame({"email": ["a@b.com", "c@d.com"]})

    def response_for_chunk(call_index, records):
        return [
            {"code": "SUCCESS", "action": "insert", "status": "success", "message": "record added", "details": {}},
            {"code": "DUPLICATE_DATA", "action": None, "status": "error", "message": "duplicate data", "details": {}},
        ]

    resource = FakeZohoCrmResource(response_for_chunk=response_for_chunk)
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_created"] == 1
    assert out["rows_errored"] == 1
    assert "first_errors" in out


def test_exception_during_upsert_counts_whole_chunk_as_errored(mod):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(3)]})

    class RaisingResource(FakeZohoCrmResource):
        def upsert(self, module_api_name, records, duplicate_check_fields=None):
            self.upsert_calls.append({"records": records})
            raise RuntimeError("boom")

    resource = RaisingResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_errored"] == 3
    assert out["rows_upserted"] == 0


def test_rows_missing_any_key_column_skipped_with_multiple_dup_fields(mod):
    df = pd.DataFrame({
        "email": ["a@b.com", "c@d.com", "e@f.com"],
        "mobile": ["111", None, "333"],
    })
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email", "Mobile"],
        fields_map={"email": "Email", "mobile": "Mobile"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "zoho_leads_out")
    # Row index 1 missing mobile -> skipped even though email is present.
    assert out["rows_skipped_no_key"] == 1
    assert out["rows_created"] == 2


def test_chunked_at_records_per_request(mod, monkeypatch):
    monkeypatch.setattr(mod, "_RECORDS_PER_REQUEST", 2)
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    # 5 rows chunked at 2 per request -> 3 calls (2, 2, 1).
    assert len(resource.upsert_calls) == 3
    assert [len(c["records"]) for c in resource.upsert_calls] == [2, 2, 1]
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_created"] == 5


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_created"] == 3


def test_empty_upstream_returns_zero_without_calling_resource(mod):
    df = pd.DataFrame({"email": pd.Series([], dtype="object")})
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_upserted"] == 0


def test_all_rows_skipped_creates_no_call(mod):
    df = pd.DataFrame({"email": [None, None]})
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.upsert_calls == []
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_skipped_no_key"] == 2


def test_source_inline_mode(mod):
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"zoho_crm": resource})
    assert result.success
    out = _metadata_for(result, "zoho_leads_out")
    assert out["rows_created"] == 2


def test_missing_required_column_raises_failure(mod):
    df = pd.DataFrame({"other_col": [1, 2]})
    resource = FakeZohoCrmResource()
    component = mod.ZohoCrmRecordUpsertComponent(
        asset_name="zoho_leads_out",
        upstream_asset_key="upstream_leads",
        module_api_name="Leads",
        duplicate_check_fields=["Email"],
        fields_map={"email": "Email"},
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)
