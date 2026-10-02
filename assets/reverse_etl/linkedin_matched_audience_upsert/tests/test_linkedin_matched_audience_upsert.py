"""Committed regression tests for LinkedInMatchedAudienceUpsertComponent.

No real network/`requests` calls are made here -- `_call_linkedin_api`
(the one external, paid-API boundary) is monkeypatched wholesale, while
hashing/normalization (the privacy-critical part), dual source
resolution, validation, chunking, and metadata are all exercised for real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeLinkedInAdsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, segment_id, api_version, elements):
        calls.append({"segment_id": segment_id, "api_version": api_version, "elements": list(elements)})
        return {}

    monkeypatch.setattr(mod, "_call_linkedin_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"linkedin_ads_resource": resource})


# --- hashing / normalization (pure, no network involved) -----------------

def test_hash_email_lowercases_and_strips_whitespace(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("Jane@Example.com") == expected
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_normalize_email_removes_internal_whitespace(mod):
    assert mod._normalize_email("ja ne@example.com") == "jane@example.com"


# --- row extraction (pure) ------------------------------------------------

def test_row_to_element_builds_sha256_email_userid(mod):
    element = mod._row_to_element({"email": "jane@example.com"}, "email", "ADD")
    assert element == {
        "action": "ADD",
        "userIds": [{"idType": "SHA256_EMAIL", "idValue": mod._hash_email("jane@example.com")}],
    }


def test_row_to_element_returns_none_for_empty(mod):
    assert mod._row_to_element({"email": None}, "email", "ADD") is None
    assert mod._row_to_element({"email": "   "}, "email", "ADD") is None


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.LinkedInMatchedAudienceUpsertComponent(
            asset_name="x",
            segment_id="10804",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.LinkedInMatchedAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            segment_id="10804",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_phone_identifier_type_rejected(mod):
    # LinkedIn's DMP Segment Users API documents no phone identifier type.
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.LinkedInMatchedAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            segment_id="10804",
            fields_map={"col": "phone"},
        ).build_defs(context=None)


def test_fields_map_without_email_raises(mod):
    with pytest.raises(ValueError, match="map exactly one column to 'email'"):
        mod.LinkedInMatchedAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            segment_id="10804",
            fields_map={},
        ).build_defs(context=None)


# --- full asset body, against the monkeypatched call ----------------------

def test_add_operation_end_to_end(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com", None, "bob@example.com"]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 1
    call = recorded_calls[0]
    assert call["segment_id"] == "10804"
    assert call["api_version"] == "202501"
    assert len(call["elements"]) == 2
    assert call["elements"][0] == {
        "action": "ADD",
        "userIds": [{"idType": "SHA256_EMAIL", "idValue": mod._hash_email("jane@example.com")}],
    }

    out = metadata_for(result, "linkedin_matched_audience_upsert_out")
    assert out["rows_submitted"] == 2
    assert out["rows_skipped_no_identifier"] == 1
    assert out["operation"] == "add"
    assert out["api_requests"] == 1


def test_remove_operation_maps_to_remove_action(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["elements"][0]["action"] == "REMOVE"


def test_all_rows_skipped_makes_no_api_call(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "linkedin_matched_audience_upsert_out")
    assert out["rows_submitted"] == 0


def test_elements_chunked_at_elements_per_request(mod, recorded_calls, monkeypatch):
    monkeypatch.setattr(mod, "_ELEMENTS_PER_REQUEST", 2)
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(5)]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    assert [len(c["elements"]) for c in recorded_calls] == [2, 2, 1]


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "linkedin_matched_audience_upsert_out")
    assert out["rows_submitted"] == 3


def test_custom_api_version_is_passed_through(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        segment_id="10804",
        fields_map={"email": "email"},
        linkedin_api_version="202601",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["api_version"] == "202601"


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeLinkedInAdsResource()
    component = mod.LinkedInMatchedAudienceUpsertComponent(
        asset_name="linkedin_matched_audience_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        segment_id="10804",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"linkedin_ads_resource": resource})
    assert result.success
    out = metadata_for(result, "linkedin_matched_audience_upsert_out")
    assert out["rows_submitted"] == 2
