"""Committed regression tests for TwitterAdsTailoredAudienceUpsertComponent.

No real network/`requests` calls are made here -- `_call_twitter_api` (the
one external, paid-API boundary) is monkeypatched wholesale, while
hashing/normalization (the privacy-critical part), row-to-user-object
building, chunking, dual source resolution, validation, and metadata are
all exercised for real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeTwitterAdsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, custom_audience_id, operation_type, users):
        calls.append(
            {
                "custom_audience_id": custom_audience_id,
                "operation_type": operation_type,
                "users": [dict(u) for u in users],
            }
        )
        return {"data": {"success_count": len(users), "total_count": len(users)}}

    monkeypatch.setattr(mod, "_call_twitter_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"twitter_ads_resource": resource})


# --- hashing / normalization (pure, no network involved) -----------------

def test_hash_email_normalizes_then_sha256(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_hash_phone_uses_e164_with_leading_plus(mod):
    # Confirmed against a worked example in X's own Ads API docs:
    # "+11234567890" -> "1fa6b8d986d9b9cd01bf36951815158bbde9f520c0567c835dfe34783d0a4231"
    # -- this only reproduces if the leading '+' is kept before hashing.
    expected = "1fa6b8d986d9b9cd01bf36951815158bbde9f520c0567c835dfe34783d0a4231"
    assert mod._hash_phone("+1 (123) 456-7890") == expected
    assert hashlib.sha256("+11234567890".encode("utf-8")).hexdigest() == expected


# --- row -> user object building (pure) -----------------------------------

def test_build_users_list_groups_identifiers_per_row_and_skips_empty(mod):
    records = [
        {"email": "a@b.com", "phone": "+14155552671"},
        {"email": None, "phone": ""},
        {"email": "c@d.com", "phone": None},
    ]
    fields_map = {"email": "email", "phone": "phone"}
    users, skipped = mod._build_users_list(records, fields_map)
    assert skipped == 1
    assert users == [
        {"email": [mod._hash_email("a@b.com")], "phone_number": [mod._hash_phone("+14155552671")]},
        {"email": [mod._hash_email("c@d.com")]},
    ]


def test_chunk_list_splits_into_expected_sizes(mod):
    chunks = list(mod._chunk_list(list(range(7)), 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6]]


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TwitterAdsTailoredAudienceUpsertComponent(
            asset_name="x",
            custom_audience_id="ztbh",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.TwitterAdsTailoredAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="ztbh",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_invalid_identifier_type_raises(mod):
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.TwitterAdsTailoredAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="ztbh",
            fields_map={"col": "maid"},
        ).build_defs(context=None)


# --- full asset body, against the monkeypatched calls ----------------------

def test_add_operation_sends_update_operation_type(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@example.com", "bob@example.com"],
            "phone": ["+14155552671", "+14155559999"],
        }
    )
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="ztbh",
        fields_map={"email": "email", "phone": "phone"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 1
    call = recorded_calls[0]
    assert call["custom_audience_id"] == "ztbh"
    assert call["operation_type"] == "Update"
    assert len(call["users"]) == 2
    assert call["users"][0]["email"] == [mod._hash_email("jane@example.com")]
    assert call["users"][0]["phone_number"] == [mod._hash_phone("+14155552671")]

    out = metadata_for(result, "twitter_audience_upsert_out")
    assert out["rows_submitted"] == 2
    assert out["requests_sent"] == 1
    assert out["operation_type"] == "Update"
    assert out["success_count"] == 2
    assert out["total_count"] == 2


def test_remove_operation_maps_to_delete(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="ztbh",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["operation_type"] == "Delete"


def test_all_rows_empty_makes_no_calls(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="ztbh",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "twitter_audience_upsert_out")
    assert out["rows_submitted"] == 0
    assert out["rows_skipped_no_identifier"] == 2


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="ztbh",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "twitter_audience_upsert_out")
    assert out["rows_submitted"] == 3


def test_requests_chunked_at_max_users_per_request(mod, recorded_calls, monkeypatch):
    # X caps each request at 2,500 users -- force a tiny cap here to exercise
    # chunking without building a 2,500-row DataFrame in a unit test.
    monkeypatch.setattr(mod, "_MAX_USERS_PER_REQUEST", 2)
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="ztbh",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3  # 2 + 2 + 1
    assert [len(c["users"]) for c in recorded_calls] == [2, 2, 1]

    out = metadata_for(result, "twitter_audience_upsert_out")
    assert out["requests_sent"] == 3
    assert out["rows_submitted"] == 5
    assert out["success_count"] == 5
    assert out["total_count"] == 5


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeTwitterAdsResource()
    component = mod.TwitterAdsTailoredAudienceUpsertComponent(
        asset_name="twitter_audience_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        custom_audience_id="ztbh",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"twitter_ads_resource": resource})
    assert result.success
    out = metadata_for(result, "twitter_audience_upsert_out")
    assert out["rows_submitted"] == 2
