"""Committed regression tests for TikTokCustomAudienceUpsertComponent.

No real network/`requests` calls are made here -- `_call_tiktok_upload` /
`_call_tiktok_update` (the two external, paid-API boundaries) are
monkeypatched wholesale, while hashing/normalization (the privacy-critical
part), per-identifier-type file grouping, dual source resolution,
validation, and metadata are all exercised for real.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeTikTokAdsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = {"uploads": [], "updates": []}

    def _fake_upload(resource, calculate_type, file_content, file_name):
        calls["uploads"].append(
            {
                "calculate_type": calculate_type,
                "file_content": file_content,
                "file_name": file_name,
            }
        )
        return f"fake-path/{file_name}"

    def _fake_update(resource, custom_audience_id, file_paths, action):
        calls["updates"].append(
            {
                "custom_audience_id": custom_audience_id,
                "file_paths": list(file_paths),
                "action": action,
            }
        )
        return {"code": 0, "message": "OK"}

    monkeypatch.setattr(mod, "_call_tiktok_upload", _fake_upload)
    monkeypatch.setattr(mod, "_call_tiktok_update", _fake_update)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"tiktok_ads_resource": resource})


# --- hashing / normalization (pure, no network involved) -----------------

def test_hash_email_normalizes_then_sha256(mod):
    expected = hashlib.sha256("jane@example.com".encode("utf-8")).hexdigest()
    assert mod._hash_email("  Jane@Example.com  ") == expected


def test_hash_phone_uses_e164_with_leading_plus(mod):
    # TikTok uses E.164 WITH a leading '+' (confirmed against TikTok's own
    # help docs) -- same convention as Google Ads, NOT Meta's digits-only one.
    expected = hashlib.sha256("+14155552671".encode("utf-8")).hexdigest()
    assert mod._hash_phone("+1 (415) 555-2671") == expected


# --- per-type grouping + file content (pure) ------------------------------

def test_collect_hashed_values_by_type_groups_and_skips_empty(mod):
    records = [
        {"email": "a@b.com", "phone": "+14155552671"},
        {"email": None, "phone": "+14155559999"},
        {"email": "c@d.com", "phone": ""},
    ]
    fields_map = {"email": "email", "phone": "phone"}
    result = mod._collect_hashed_values_by_type(records, fields_map)
    assert result["email"] == [mod._hash_email("a@b.com"), mod._hash_email("c@d.com")]
    assert result["phone"] == [mod._hash_phone("+14155552671"), mod._hash_phone("+14155559999")]


def test_build_identifier_file_content_has_header_then_one_per_line(mod):
    content = mod._build_identifier_file_content("email", ["hash1", "hash2"])
    assert content == "Email_SHA256\nhash1\nhash2\n"


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TikTokCustomAudienceUpsertComponent(
            asset_name="x",
            custom_audience_id="123",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.TikTokCustomAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="123",
            fields_map={"email": "email"},
            operation="upsert",
        ).build_defs(context=None)


def test_invalid_identifier_type_raises(mod):
    with pytest.raises(ValueError, match="unsupported identifier"):
        mod.TikTokCustomAudienceUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            custom_audience_id="123",
            fields_map={"col": "maid"},
        ).build_defs(context=None)


# --- full asset body, against the monkeypatched calls ----------------------

def test_add_operation_uploads_one_file_per_type_then_updates_once(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@example.com", "bob@example.com"],
            "phone": ["+14155552671", "+14155559999"],
        }
    )
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email", "phone": "phone"},
        operation="add",
    )
    result = _materialize(component, df, resource)
    assert result.success

    uploads = recorded_calls["uploads"]
    assert len(uploads) == 2
    calc_types = {u["calculate_type"] for u in uploads}
    assert calc_types == {"EMAIL_SHA256", "PHONE_SHA256"}
    email_upload = next(u for u in uploads if u["calculate_type"] == "EMAIL_SHA256")
    assert email_upload["file_content"] == (
        "Email_SHA256\n" + mod._hash_email("jane@example.com") + "\n" + mod._hash_email("bob@example.com") + "\n"
    )

    updates = recorded_calls["updates"]
    assert len(updates) == 1
    assert updates[0]["custom_audience_id"] == "999888777"
    assert updates[0]["action"] == "APPEND"
    assert len(updates[0]["file_paths"]) == 2

    out = metadata_for(result, "tiktok_custom_audience_upsert_out")
    assert out["rows_submitted"] == 4  # 2 emails + 2 phones, counted per identifier
    assert out["files_uploaded"] == 2
    assert out["action"] == "APPEND"


def test_remove_operation_maps_to_remove_action(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email"},
        operation="remove",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls["updates"][0]["action"] == "REMOVE"


def test_only_present_identifier_types_get_uploaded(mod, recorded_calls):
    # fields_map declares both, but only email has any data -- only 1 upload.
    df = pd.DataFrame({"email": ["jane@example.com"], "phone": [None]})
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email", "phone": "phone"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls["uploads"]) == 1
    assert recorded_calls["uploads"][0]["calculate_type"] == "EMAIL_SHA256"


def test_all_rows_empty_makes_no_calls(mod, recorded_calls):
    df = pd.DataFrame({"email": [None, ""]})
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls["uploads"] == []
    assert recorded_calls["updates"] == []
    out = metadata_for(result, "tiktok_custom_audience_upsert_out")
    assert out["rows_submitted"] == 0


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "tiktok_custom_audience_upsert_out")
    assert out["rows_submitted"] == 3


def test_small_file_warns_but_still_uploads(mod, recorded_calls):
    # Well under TikTok's recommended 1,000-entry minimum -- should warn,
    # not fail or skip the upload.
    df = pd.DataFrame({"email": ["jane@example.com"]})
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        upstream_asset_key="upstream_customers",
        custom_audience_id="999888777",
        fields_map={"email": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls["uploads"]) == 1


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeTikTokAdsResource()
    component = mod.TikTokCustomAudienceUpsertComponent(
        asset_name="tiktok_custom_audience_upsert_out",
        source={"kind": "inline", "rows": [{"email": "a@b.com"}, {"email": "c@d.com"}]},
        custom_audience_id="999888777",
        fields_map={"email": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"tiktok_ads_resource": resource})
    assert result.success
    out = metadata_for(result, "tiktok_custom_audience_upsert_out")
    assert out["rows_submitted"] == 2
