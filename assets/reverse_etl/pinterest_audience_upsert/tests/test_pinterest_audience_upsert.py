"""Committed regression tests for PinterestAudienceUpsertComponent.

No real network/`requests` calls are made here -- `_call_pinterest_update`
(the one external, paid-API boundary) is monkeypatched wholesale, while
hashing/normalization (the privacy-critical part), row-to-record building,
dual source resolution, validation, and metadata are all exercised for
real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakePinterestAdsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, customer_list_id, operation_type, records_v2):
        calls.append(
            {
                "customer_list_id": customer_list_id,
                "operation_type": operation_type,
                "records_v2": [dict(r) for r in records_v2],
            }
        )
        return {
            "status": "PROCESSING",
            "num_uploaded_user_records": len(records_v2) if operation_type == "ADD" else 0,
            "num_removed_user_records": len(records_v2) if operation_type == "REMOVE" else 0,
            "num_batches": 1,
        }

    monkeypatch.setattr(mod, "_call_pinterest_update", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"pinterest_ads_resource": resource})


# --- hashing / normalization (pure, no network involved) -----------------

def test_hash_email_normalizes_then_sha256(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_hash_phone_is_digits_only_no_plus(mod):
    # Pinterest's own conversions/enhanced-match docs: "only digits with
    # country code, area code, and number... with any symbols, letters,
    # spaces and leading zeros removed" -- NOT E.164 with a leading '+'
    # (that's Google Ads/TikTok/X's convention, not Pinterest's).
    expected = hashlib.sha256("11234567890".encode("utf-8")).hexdigest()
    assert mod._hash_phone("+1 (123) 456-7890") == expected
    # A leading-zero variant normalizes to the same digits, per Pinterest's
    # documented "leading zeros removed" rule.
    assert mod._hash_phone("011234567890") == expected


def test_normalize_phone_strips_plus_unlike_other_platforms(mod):
    assert mod._normalize_phone("+14155552671") == "14155552671"


# --- row -> records_v2 building (pure) -------------------------------------

def test_build_records_v2_groups_identifiers_per_row_and_skips_empty(mod):
    records = [
        {"email": "a@b.com", "phone": "+14155552671"},
        {"email": None, "phone": ""},
        {"email": "c@d.com", "phone": None},
    ]
    fields_map = {"email": "email", "phone": "phone"}
    records_v2, skipped = mod._build_records_v2(records, fields_map)
    assert skipped == 1
    assert records_v2 == [
        {"email": mod._hash_email("a@b.com"), "hashed_phone_number": mod._hash_phone("+14155552671")},
        {"email": mod._hash_email("c@d.com")},
    ]


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.PinterestAudienceUpsertComponent(
            asset_name="x",
            customer_list_id="643",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.PinterestAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            customer_list_id="643",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_invalid_identifier_type_raises(mod):
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.PinterestAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            customer_list_id="643",
            fields_map={"col": "maid"},
        ).build_defs(context=None)


# --- full asset body, against the monkeypatched calls ----------------------

def test_add_operation_sends_add_operation_type(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@example.com", "bob@example.com"],
            "phone": ["+14155552671", "+14155559999"],
        }
    )
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        customer_list_id="643",
        fields_map={"email": "email", "phone": "phone"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 1
    call = recorded_calls[0]
    assert call["customer_list_id"] == "643"
    assert call["operation_type"] == "ADD"
    assert len(call["records_v2"]) == 2
    assert call["records_v2"][0]["email"] == mod._hash_email("jane@example.com")
    assert call["records_v2"][0]["hashed_phone_number"] == mod._hash_phone("+14155552671")

    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["rows_submitted"] == 2
    assert out["operation_type"] == "ADD"
    assert out["status"] == "PROCESSING"
    assert out["num_uploaded_user_records"] == 2


def test_remove_operation_maps_to_remove(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        customer_list_id="643",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["operation_type"] == "REMOVE"
    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["num_removed_user_records"] == 1


def test_all_rows_empty_makes_no_calls(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        customer_list_id="643",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["rows_submitted"] == 0
    assert out["rows_skipped_no_identifier"] == 2


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        customer_list_id="643",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        customer_list_id="643",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"pinterest_ads_resource": resource})
    assert result.success
    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["rows_submitted"] == 2


def test_too_small_status_still_reported_as_success(mod, recorded_calls, monkeypatch):
    # A successful upload with fewer than 100 matched Pinterest accounts
    # reports status=TOO_SMALL -- that's not an error from this component's
    # perspective, just a visible signal in the metadata.
    def _fake_call(resource, customer_list_id, operation_type, records_v2):
        return {
            "status": "TOO_SMALL",
            "num_uploaded_user_records": len(records_v2),
            "num_removed_user_records": 0,
            "num_batches": 1,
        }

    monkeypatch.setattr(mod, "_call_pinterest_update", _fake_call)
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakePinterestAdsResource()
    component = mod.PinterestAudienceUpsertComponent(
        asset_name="pinterest_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        customer_list_id="643",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "pinterest_audience_upsert_out")
    assert out["status"] == "TOO_SMALL"
