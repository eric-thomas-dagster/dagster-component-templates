"""Committed regression tests for GoogleAdsCustomerMatchUpsertComponent.

The real `google-ads` SDK is never installed or imported here -- a minimal
fake OfflineUserDataJobService (conftest.py) stands in for the one
external, paid-API boundary, while everything this component actually
owns -- hashing/normalization (the privacy-critical part), dual source
resolution, validation, chunking, and metadata -- is exercised for real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeGoogleAdsResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"google_ads_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- hashing / normalization (pure, no SDK involved) --------------------

def test_hash_email_normalizes_then_sha256(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_hash_phone_keeps_leading_plus_strips_rest(mod):
    expected = hashlib.sha256("+14155552671".encode("utf-8")).hexdigest()
    assert mod._hash_phone("+1 (415) 555-2671") == expected


def test_hash_phone_without_leading_plus_still_adds_one(mod):
    # Google requires E.164 (leading +); a bare digit string gets one added
    # rather than silently producing a hash that will never match.
    expected = hashlib.sha256("+14155552671".encode("utf-8")).hexdigest()
    assert mod._hash_phone("14155552671") == expected


def test_extract_identifiers_skips_empty_and_null(mod):
    row = {"email_col": "a@b.com", "phone_col": None, "unused_col": "x"}
    fields_map = {"email_col": "email", "phone_col": "phone"}
    result = mod._extract_identifiers_for_row(row, fields_map)
    assert result == [("email", mod._hash_email("a@b.com"))]


def test_extract_identifiers_multiple_per_row(mod):
    row = {"email_col": "a@b.com", "phone_col": "+14155552671"}
    fields_map = {"email_col": "email", "phone_col": "phone"}
    result = mod._extract_identifiers_for_row(row, fields_map)
    assert result == [
        ("email", mod._hash_email("a@b.com")),
        ("phone", mod._hash_phone("+14155552671")),
    ]


def test_extract_identifiers_blank_string_skipped(mod):
    row = {"email_col": "   ", "phone_col": "+14155552671"}
    fields_map = {"email_col": "email", "phone_col": "phone"}
    result = mod._extract_identifiers_for_row(row, fields_map)
    assert result == [("phone", mod._hash_phone("+14155552671"))]


# --- validation ----------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.GoogleAdsCustomerMatchUpsertComponent(
            asset_name="x",
            user_list_resource_name="customers/1/userLists/2",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.GoogleAdsCustomerMatchUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            user_list_resource_name="customers/1/userLists/2",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.GoogleAdsCustomerMatchUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            user_list_resource_name="customers/1/userLists/2",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_invalid_identifier_type_raises(mod):
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.GoogleAdsCustomerMatchUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            user_list_resource_name="customers/1/userLists/2",
            fields_map={"col": "mobile_id"},
        ).build_defs(context=None)


# --- full asset body, against the fake SDK -------------------------------

def test_add_operation_end_to_end(mod):
    df = pd.DataFrame(
        {
            "email": ["jane@example.com", None, "bob@example.com"],
            "phone": ["+14155552671", "+14155559999", None],
        }
    )
    resource = FakeGoogleAdsResource(customer_id="1112223333")
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email", "phone": "phone"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    svc = resource.client.offline_user_data_job_service
    assert len(svc.create_calls) == 1
    create_call = svc.create_calls[0]
    assert create_call["customer_id"] == "1112223333"
    assert create_call["job"].customer_match_user_list_metadata.user_list == "customers/1112223333/userLists/999"
    assert create_call["job"].type_ == "CUSTOMER_MATCH_USER_LIST"

    assert len(svc.add_calls) == 1
    add_request = svc.add_calls[0]
    assert add_request.resource_name == create_call["resource_name"]
    assert add_request.enable_partial_failure is True
    # All 3 rows have at least one identifier -- 0 skipped.
    assert len(add_request.operations) == 3
    # Every identifier landed on `.create`, never `.remove`.
    for op in add_request.operations:
        assert len(op.remove.user_identifiers) == 0
    first_op_identifiers = add_request.operations[0].create.user_identifiers
    hashed_values = {ui.hashed_email for ui in first_op_identifiers if ui.hashed_email} | {
        ui.hashed_phone_number for ui in first_op_identifiers if ui.hashed_phone_number
    }
    assert mod._hash_email("jane@example.com") in hashed_values
    assert mod._hash_phone("+14155552671") in hashed_values

    assert svc.run_calls == [create_call["resource_name"]]

    out = _metadata_for(result, "google_ads_customer_match_upsert_out")
    assert out["rows_submitted"] == 3
    assert out["rows_skipped_no_identifier"] == 0
    assert out["operation"] == "add"


def test_remove_operation_targets_remove_not_create(mod):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    svc = resource.client.offline_user_data_job_service
    op = svc.add_calls[0].operations[0]
    assert len(op.create.user_identifiers) == 0
    assert len(op.remove.user_identifiers) == 1


def test_rows_with_no_identifier_are_skipped_and_counted(mod):
    df = pd.DataFrame({"email": ["jane@example.com", None, ""]})
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "google_ads_customer_match_upsert_out")
    assert out["rows_submitted"] == 1
    assert out["rows_skipped_no_identifier"] == 2


def test_all_rows_skipped_creates_no_job(mod):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    svc = resource.client.offline_user_data_job_service
    assert svc.create_calls == []
    out = _metadata_for(result, "google_ads_customer_match_upsert_out")
    assert out["rows_submitted"] == 0
    assert out["rows_skipped_no_identifier"] == 2


def test_operations_chunked_at_operations_per_request(mod, monkeypatch):
    monkeypatch.setattr(mod, "_OPERATIONS_PER_REQUEST", 2)
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    svc = resource.client.offline_user_data_job_service
    # 5 rows chunked at 2 per request -> 3 requests (2, 2, 1).
    assert len(svc.add_calls) == 3
    assert [len(r.operations) for r in svc.add_calls] == [2, 2, 1]
    # Still exactly one job created and one run call, regardless of chunking.
    assert len(svc.create_calls) == 1
    assert len(svc.run_calls) == 1


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        upstream_asset_key="upstream_customers",
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "google_ads_customer_match_upsert_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod):
    resource = FakeGoogleAdsResource()
    component = mod.GoogleAdsCustomerMatchUpsertComponent(
        asset_name="google_ads_customer_match_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        user_list_resource_name="customers/1112223333/userLists/999",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"google_ads_resource": resource})
    assert result.success
    out = _metadata_for(result, "google_ads_customer_match_upsert_out")
    assert out["rows_submitted"] == 2
