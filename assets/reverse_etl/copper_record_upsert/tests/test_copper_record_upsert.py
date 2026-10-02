"""Committed regression tests for CopperRecordUpsertComponent.

The real Copper API is never hit -- FakeCopperResource (conftest.py)
stands in for the one external-API boundary (`.upsert()`), while
everything this component actually owns -- object-type-specific
body/filter shaping (the privacy/correctness-critical part, since
Copper's shapes genuinely differ by object_type), dual source
resolution, validation, row iteration, and metadata -- is exercised for
real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeCopperResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_contacts", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"copper": resource})


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- pure shaping helpers (no SDK / resource involved) -----------------------

def test_build_write_body_people_wraps_email_and_phone(mod):
    body = mod._build_write_body(
        "people", {"name": "Jane", "email": "jane@example.com", "phone": "415-555-0100"}
    )
    assert body == {
        "name": "Jane",
        "emails": [{"email": "jane@example.com", "category": "work"}],
        "phone_numbers": [{"number": "415-555-0100", "category": "work"}],
    }


def test_build_write_body_leads_uses_singular_email_object(mod):
    body = mod._build_write_body("leads", {"name": "Jim", "email": "jim@example.com"})
    # Leads use a SINGULAR email object, not an array like People.
    assert body == {"name": "Jim", "email": {"email": "jim@example.com", "category": "work"}}


def test_build_write_body_companies_maps_email_to_email_domain(mod):
    body = mod._build_write_body("companies", {"name": "Acme", "email": "acme.com"})
    # Companies have no emails/email field at all -- only email_domain.
    assert body == {"name": "Acme", "email_domain": "acme.com"}


def test_build_write_body_phone_shape_identical_across_object_types(mod):
    for object_type in ("people", "leads", "companies"):
        body = mod._build_write_body(object_type, {"phone": "415-555-0100"})
        assert body == {"phone_numbers": [{"number": "415-555-0100", "category": "work"}]}


def test_build_write_body_passthrough_scalar_fields(mod):
    body = mod._build_write_body("companies", {"name": "Acme", "email_domain": "acme.com", "details": "notes"})
    assert body == {"name": "Acme", "email_domain": "acme.com", "details": "notes"}


def test_build_search_filter_people_email_is_array(mod):
    assert mod._build_search_filter("people", "email", "jane@example.com") == {
        "emails": ["jane@example.com"]
    }


def test_build_search_filter_leads_email_is_bare_string(mod):
    # Real Copper inconsistency: Leads search filter is a bare string, not a list.
    assert mod._build_search_filter("leads", "email", "jim@example.com") == {
        "emails": "jim@example.com"
    }


def test_build_search_filter_companies_uses_plural_domains_key(mod):
    assert mod._build_search_filter("companies", "email_domain", "acme.com") == {
        "email_domains": "acme.com"
    }
    assert mod._build_search_filter("companies", "email", "acme.com") == {
        "email_domains": "acme.com"
    }


def test_build_search_filter_generic_field_passthrough(mod):
    assert mod._build_search_filter("people", "name", "Jane Doe") == {"name": "Jane Doe"}


# --- validation ----------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.CopperRecordUpsertComponent(
            asset_name="x",
            object_type="people",
            dedupe_field="email",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.CopperRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            object_type="people",
            dedupe_field="email",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_invalid_object_type_raises(mod):
    with pytest.raises(ValueError, match="object_type"):
        mod.CopperRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            object_type="deals",
            dedupe_field="email",
            fields_map={"email": "email"},
        ).build_defs(context=None)


def test_dedupe_field_not_in_fields_map_raises(mod):
    with pytest.raises(ValueError, match="dedupe_field"):
        mod.CopperRecordUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            object_type="people",
            dedupe_field="email",
            fields_map={"full_name": "name"},
        ).build_defs(context=None)


# --- full asset body, against the fake resource -------------------------------

def test_creates_new_records_end_to_end(mod):
    df = pd.DataFrame(
        {
            "full_name": ["Jane", "Bob"],
            "email_address": ["jane@example.com", "bob@example.com"],
            "phone": ["415-555-0100", "415-555-0101"],
        }
    )
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        upstream_asset_key="upstream_contacts",
        object_type="people",
        dedupe_field="email",
        fields_map={"full_name": "name", "email_address": "email", "phone": "phone"},
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.upsert_calls) == 2
    first_call = resource.upsert_calls[0]
    assert first_call["object_type"] == "people"
    assert first_call["search_filter"] == {"emails": ["jane@example.com"]}
    assert first_call["create_body"] == {
        "name": "Jane",
        "emails": [{"email": "jane@example.com", "category": "work"}],
        "phone_numbers": [{"number": "415-555-0100", "category": "work"}],
    }

    out = _metadata_for(result, "copper_people_out")
    assert out["rows_created"] == 2
    assert out["rows_updated"] == 0
    assert out["rows_upserted"] == 2
    assert out["rows_errored"] == 0
    assert out["rows_skipped_no_key"] == 0
    assert out["copper_object_type"] == "people"


def test_updates_existing_record(mod):
    df = pd.DataFrame({"email_address": ["jane@example.com"], "full_name": ["Jane Updated"]})
    resource = FakeCopperResource()
    # Pre-seed the fake store so this email already "exists" in Copper.
    resource._store[("people", "jane@example.com")] = 555

    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        upstream_asset_key="upstream_contacts",
        object_type="people",
        dedupe_field="email",
        fields_map={"full_name": "name", "email_address": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "copper_people_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_leads_object_type_uses_singular_email_shape_end_to_end(mod):
    df = pd.DataFrame({"email_address": ["jim@example.com"], "full_name": ["Jim"]})
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_leads_out",
        upstream_asset_key="upstream_contacts",
        object_type="leads",
        dedupe_field="email",
        fields_map={"full_name": "name", "email_address": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    call = resource.upsert_calls[0]
    assert call["search_filter"] == {"emails": "jim@example.com"}  # bare string, not a list
    assert call["create_body"] == {"name": "Jim", "email": {"email": "jim@example.com", "category": "work"}}


def test_rows_missing_dedupe_value_are_skipped_and_counted(mod):
    df = pd.DataFrame({"email_address": ["jane@example.com", None, ""], "full_name": ["Jane", "No Email", "Blank"]})
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        upstream_asset_key="upstream_contacts",
        object_type="people",
        dedupe_field="email",
        fields_map={"full_name": "name", "email_address": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "copper_people_out")
    # Note: "" is falsy-but-not-None -- only true None/NaN are skipped by this
    # component's _row_value helper, so "" still attempts a write.
    assert out["rows_skipped_no_key"] == 1
    assert len(resource.upsert_calls) == 2


def test_errors_are_caught_counted_and_reported(mod):
    df = pd.DataFrame({"email_address": ["boom@example.com"], "full_name": ["Boom"]})

    class RaisingResource(FakeCopperResource):
        def upsert(self, *args, **kwargs):
            raise RuntimeError("Copper API 500")

    resource = RaisingResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        upstream_asset_key="upstream_contacts",
        object_type="people",
        dedupe_field="email",
        fields_map={"full_name": "name", "email_address": "email"},
    )
    result = _materialize(component, df, resource)
    assert result.success  # component catches per-row errors, doesn't fail the asset
    out = _metadata_for(result, "copper_people_out")
    assert out["rows_errored"] == 1
    assert out["rows_upserted"] == 0
    assert "first_errors" in out


def test_batch_size_caps_rows(mod):
    df = pd.DataFrame({"email_address": [f"user{i}@example.com" for i in range(10)]})
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        upstream_asset_key="upstream_contacts",
        object_type="people",
        dedupe_field="email",
        fields_map={"email_address": "email"},
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "copper_people_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod):
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_people_out",
        source={"kind": "inline", "rows": [{"email_address": "a@b.com"}, {"email_address": "c@d.com"}]},
        object_type="people",
        dedupe_field="email",
        fields_map={"email_address": "email"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"copper": resource})
    assert result.success
    out = _metadata_for(result, "copper_people_out")
    assert out["rows_upserted"] == 2


def test_companies_object_type_requires_email_domain_mapping(mod):
    df = pd.DataFrame({"account_name": ["Acme"], "domain": ["acme.com"]})
    resource = FakeCopperResource()
    component = mod.CopperRecordUpsertComponent(
        asset_name="copper_companies_out",
        upstream_asset_key="upstream_contacts",
        object_type="companies",
        dedupe_field="email_domain",
        fields_map={"account_name": "name", "domain": "email_domain"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    call = resource.upsert_calls[0]
    assert call["search_filter"] == {"email_domains": "acme.com"}
    assert call["create_body"] == {"name": "Acme", "email_domain": "acme.com"}
