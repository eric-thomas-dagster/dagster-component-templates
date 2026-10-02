"""Committed regression tests for MetaCustomAudienceUpsertComponent.

The real `facebook-business` SDK is never installed or imported here --
`_call_facebook_api` (the one external, paid-API boundary) is monkeypatched
wholesale, while hashing/normalization (the privacy-critical part), schema
building, dual source resolution, validation, chunking, and metadata are
all exercised for real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeFacebookAdsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, custom_audience_id, schema, rows, operation):
        calls.append(
            {
                "resource": resource,
                "custom_audience_id": custom_audience_id,
                "schema": list(schema),
                "rows": [list(r) for r in rows],
                "operation": operation,
            }
        )
        return {"num_received": len(rows)}

    monkeypatch.setattr(mod, "_call_facebook_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"facebook_ads_resource": resource})


# --- hashing / normalization (pure, no SDK involved) --------------------

def test_hash_email_normalizes_then_sha256(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_hash_phone_strips_plus_and_symbols(mod):
    # Meta wants DIGITS ONLY -- no leading '+', unlike Google Ads' E.164 form.
    expected = hashlib.sha256("14155552671".encode("utf-8")).hexdigest()
    assert mod._hash_phone("+1 (415) 555-2671") == expected


# --- schema building + positional row extraction (pure) -----------------

def test_build_schema_dedupes_and_preserves_order(mod):
    fields_map = {"phone_col": "phone", "email_col": "email", "email_col2": "email"}
    assert mod._build_schema(fields_map) == ["PHONE", "EMAIL"]


def test_row_to_schema_values_positional_with_missing_as_empty_string(mod):
    schema = ["EMAIL", "PHONE"]
    fields_map = {"email_col": "email", "phone_col": "phone"}
    row = {"email_col": "a@b.com", "phone_col": None}
    result = mod._row_to_schema_values(row, fields_map, schema)
    assert result == [mod._hash_email("a@b.com"), ""]


def test_row_to_schema_values_returns_none_when_all_empty(mod):
    schema = ["EMAIL", "PHONE"]
    fields_map = {"email_col": "email", "phone_col": "phone"}
    row = {"email_col": None, "phone_col": "   "}
    assert mod._row_to_schema_values(row, fields_map, schema) is None


# --- validation ----------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.MetaCustomAudienceUpsertComponent(
            asset_name="x",
            custom_audience_id="123",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.MetaCustomAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="123",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_invalid_identifier_type_raises(mod):
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.MetaCustomAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="123",
            fields_map={"col": "madid"},
        ).build_defs(context=None)


# --- full asset body, against the monkeypatched API call -----------------

def test_add_operation_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@example.com", None, "bob@example.com"],
            "phone": ["+14155552671", "+14155559999", None],
        }
    )
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email", "phone": "phone"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 1
    call = recorded_calls[0]
    assert call["custom_audience_id"] == "23850000000000000"
    assert call["operation"] == "add"
    assert call["schema"] == ["EMAIL", "PHONE"]
    assert len(call["rows"]) == 3  # all 3 rows have at least one identifier
    assert call["rows"][0] == [mod._hash_email("jane@example.com"), mod._hash_phone("+14155552671")]
    # Row 2 (null email) still contributes its phone, with "" in the EMAIL slot.
    assert call["rows"][1][0] == ""
    assert call["rows"][1][1] == mod._hash_phone("+14155559999")

    out = metadata_for(result, "meta_custom_audience_upsert_out")
    assert out["rows_submitted"] == 3
    assert out["rows_skipped_no_identifier"] == 0
    assert out["operation"] == "add"
    assert out["api_requests"] == 1


def test_remove_operation_passes_through(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["operation"] == "remove"


def test_rows_with_no_identifier_are_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com", None, ""]})
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "meta_custom_audience_upsert_out")
    assert out["rows_submitted"] == 1
    assert out["rows_skipped_no_identifier"] == 2


def test_all_rows_skipped_makes_no_api_call(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "meta_custom_audience_upsert_out")
    assert out["rows_submitted"] == 0


def test_rows_chunked_at_rows_per_request(mod, recorded_calls, monkeypatch):
    monkeypatch.setattr(mod, "_ROWS_PER_REQUEST", 2)
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    assert [len(c["rows"]) for c in recorded_calls] == [2, 2, 1]


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "meta_custom_audience_upsert_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeFacebookAdsResource()
    component = mod.MetaCustomAudienceUpsertComponent(
        asset_name="meta_custom_audience_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        custom_audience_id="23850000000000000",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"facebook_ads_resource": resource})
    assert result.success
    out = metadata_for(result, "meta_custom_audience_upsert_out")
    assert out["rows_submitted"] == 2
